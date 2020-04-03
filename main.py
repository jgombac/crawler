from time import sleep

from db_classes import *
from utils import *
from url_normalize import url_normalize
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from socket import gethostbyname, gaierror
import concurrent.futures
import threading
import time
import sys


import warnings
warnings.filterwarnings("ignore")
page_selection_lock = threading.Lock()
delay_lock = threading.Lock()
ACTIVE_THREADS = 0
DEFAULT_REQUEST_DELAY = 5

CONNECTION_STRING = "postgres://postgres:postgres@192.168.99.100:5432/crawldb"
ENGINE = create_engine(CONNECTION_STRING, echo=False, connect_args={'options': '-csearch_path=crawldb'})
Session = scoped_session(sessionmaker(bind=ENGINE))
dbGlobal = Session()
dbGlobal.execute("SET search_path TO crawldb")

site_seeds = ["www.gov.si", "evem.gov.si", "e-uprava.gov.si", "e-prostor.gov.si"]


def get_first_in_queue(db):
    with page_selection_lock:
        crawling_results = []
        try:
            current_crawling = db.execute("select site_id from crawldb.page where page_type_code = 'CRAWLING'")
            crawling_results = [dict(row) for row in current_crawling]
        except Exception as e:
            crawling_results = []
            print("CRAWLING SELECT failed", e)
            db.rollback()
        not_available = [pg['site_id'] for pg in crawling_results]

        depth = db.query(Page).filter(Page.page_type_code == "FRONTIER").order_by(Page.depth).first().depth

        page = db\
            .query(Page) \
            .filter(and_(Page.page_type_code == "FRONTIER", Page.depth <= depth, Page.site_id.notin_(not_available)))\
            .first()

        while not page:
            page = db \
                .query(Page) \
                .filter(
                and_(Page.page_type_code == "FRONTIER", Page.depth <= depth)) \
                .first()
            depth += 1

            if depth > 10:
                return None
        page.page_type_code = "CRAWLING"
        db.commit()
    return page


def crawl_page(page, db, browser):
    if page.page_type_code != "CRAWLING":
        return

    delay = page.site.get_robots().crawl_delay(USER_AGENT)
    if not delay:
        delay = DEFAULT_REQUEST_DELAY

    # Don't crawl the site if we can't find it's IP
    should_wait = wait_before_crawling(page, delay, db)
    if should_wait is False:
        page.page_type_code = "SKIP"
        db.commit()
        return

    if should_wait is None:
        page.page_type_code = "FRONTIER"
        db.commit()
        return

    try:
        page.retrieve_page(db, browser)
    except Exception as ex:
        pass
    finally:
        db.commit()


def crawl():
    db = Session()
    browser = get_browser()
    db.execute("SET search_path TO crawldb")
    page = get_first_in_queue(db)
    while page:
        try:
            crawl_page(page, db, browser)
        except Exception as ex:
            print(f"{threading.currentThread().ident}: ERROR crawling page {page.url} \n {ex}")
            page.page_type_code = "ERROR"
            db.commit()
            browser.quit()
            browser = get_browser()

        page = get_first_in_queue(db)

    browser.quit()


def wait_before_crawling(page: Page, delay, db):
    """
    Checks when a page from this IP was last crawled and waits for the
    crawl delay if necessary.
    :return: False if IP could not be looked up, True after successfully waiting
    """
    with delay_lock:
        try:
            page_ip = gethostbyname(page.get_domain())
            visited_ip = db.query(VisitedIP).filter(VisitedIP.ip == page_ip).first()

            current_time = get_current_timestamp()

            if not visited_ip:
                visited_ip = VisitedIP(ip=page_ip, last_visited=timestamp_to_date(current_time))
                db.add(visited_ip)

                # If IP hasn't been visited yet, we don't need to wait
                return True

            time_elapsed = current_time - date_to_timestamp(visited_ip.last_visited)

            if time_elapsed < delay:
                wait_time = delay - time_elapsed
                return None
            else:
                pass

            visited_ip.last_visited = get_current_datetime()
            db.commit()

            return True

        except gaierror as e:
            print("ERROR retrieving domain IP")
            print(e)
            return False


def run_workers(num):

    with concurrent.futures.ThreadPoolExecutor(max_workers=num) as executor:
        results = []

        while True:
            while len(executor._threads) < num:
                time.sleep(1)
                results.append(executor.submit(crawl))
            time.sleep(1)
            concurrent.futures.wait(results, timeout=30, return_when=concurrent.futures.ALL_COMPLETED)


if __name__ == "__main__":
    num_workers = 6
    if len(sys.argv) > 1:
        num_workers = int(sys.argv[1])

    print(f"Running crawler with {num_workers} workers.")

    crawling = dbGlobal.query(Page).filter(Page.page_type_code == "CRAWLING").all()
    for pg in crawling:
        pg.page_type_code = "FRONTIER"
    dbGlobal.commit()

    for seed in site_seeds:
        Page.find_or_create_page(seed, dbGlobal, 0)

    run_workers(num_workers)

    print("Done?")
