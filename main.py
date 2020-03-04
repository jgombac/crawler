from time import sleep

from db_classes import *
from utils import *
from url_normalize import url_normalize
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from socket import gethostbyname, gaierror

import warnings
warnings.filterwarnings("ignore")

DEFAULT_REQUEST_DELAY = 5

CONNECTION_STRING = "postgres://postgres:postgres@localhost:5432/crawldb"
ENGINE = create_engine(CONNECTION_STRING, echo=False)
Session = sessionmaker(bind=ENGINE)
db = Session()
db.execute("SET search_path TO crawldb")


site_seeds = ["gov.si"]  # "evem.gov.si", "e-uprava.gov.si", "e-prostor.gov.si"]


def get_first_in_queue():
    page = db\
        .query(Page)\
        .filter(and_(Page.page_type_code == "FRONTIER", Page.http_status_code == None))\
        .order_by(Page.id).first()
    return page


def crawl_pages(urls):
    for url in urls:
        url = url_normalize(url)
        domain = get_domain(url)
        site = db.query(Site).filter(Site.domain == domain).first()

        if not site:
            site = Site(domain=domain)
            db.add(site)
        if not site.robots_content:
            site.retrieve_site_robots()

        if not site.get_robots().can_fetch(USER_AGENT, url):
            db.commit()
            return

        page = db.query(Page).filter(Page.url == url).first()

        if not page:
            page = Page(url=url, page_type_code="FRONTIER")
            db.add(page)

        if page.page_type_code != "FRONTIER":
            db.commit()
            return

        delay = site.get_robots().crawl_delay(USER_AGENT)
        if not delay:
            delay = DEFAULT_REQUEST_DELAY

        # Don't crawl the site if we can't find it's IP
        if not wait_before_crawling(page, delay):
            db.commit()
            return

        page.retrieve_page(db)
        db.commit()


def crawl():
    page = get_first_in_queue()

    while page:
        crawl_pages([page.url])
        page = get_first_in_queue()


def wait_before_crawling(page: Page, delay):
    """
    Checks when a page from this IP was last crawled and waits for the
    crawl delay if necessary.
    :return: False if IP could not be looked up, True after successfully waiting
    """

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
            print(f"Waiting for {wait_time:.2f} seconds before crawling {page.url}")
            sleep(wait_time)
        else:
            print(f"{time_elapsed} seconds have elapsed since fetching {visited_ip.ip}. Crawling...")

        visited_ip.last_visited = get_current_datetime()

        return True

    except gaierror as e:
        print(e)
        return False


if __name__ == "__main__":
    crawl_pages(site_seeds)
    crawl()
    db.close()

