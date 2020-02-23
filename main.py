from db_classes import *
from utils import *
from url_normalize import url_normalize
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.sql import exists

import warnings
warnings.filterwarnings("ignore")


CONNECTION_STRING = "postgres://postgres:postgres@localhost:5432/crawldb"
ENGINE = create_engine(CONNECTION_STRING, echo=True)
Session = sessionmaker(bind=ENGINE)
db = Session()
db.execute("SET search_path TO crawldb")




site_seeds = ["gov.si"]  # "evem.gov.si", "e-uprava.gov.si", "e-prostor.gov.si"]


def get_first_in_queue():
    page = db.query(Page).filter(and_(Page.page_type_code == "FRONTIER", Page.http_status_code == None)).order_by(Page.id).first()
    return page


def crawl_page(url):
    url = url_normalize(url)
    domain = get_domain(url)
    site = db.query(Site).filter(Site.domain == domain).first()

    if not site:
        site = Site(domain=domain)
        db.add(site)
    if not site.robots_content:
        site.retrieve_site_robots()

    if not site.get_robots().can_fetch(USER_AGENT, url):
        return

    page = db.query(Page).filter(Page.url == url).first()


    if not page:
        page = Page(url=url, page_type_code="FRONTIER")
        db.add(page)

    if page.page_type_code != "FRONTIER":
        return

    page.retrieve_page(db)
    db.commit()


def crawl():
    page = get_first_in_queue()

    while page:
        crawl_page(page.url)
        page = get_first_in_queue()





if __name__ == "__main__":
    crawl_page("gov.si")
    crawl()
    # get_first_in_queue()


    # for site in site_seeds:
    #     url = url_normalize(site)
    #     page = db.query(Page).filter(exists().where(Page.url == url)).first()
    #     if not page:
    #         domain = get_domain(url)
    #         site = db.query(Site).filter(exists().where(Site.domain == domain)).first()
    #         if not site:
    #             site = Site(domain=domain)
    #             db.add(site)
    #         if not site.robots_content:
    #             site.retrieve_site_robots()
    #         page = Page(url=url, page_type_code="FRONTIER")
    #         db.add(page)
    #     db.commit()





    # for site in db.query(Site):
    #     site.retrieve_site_robots()
    #     db.commit()

    db.close()

    # for site_url in site_seeds:
    #     site = Site(site_url)
    #
    #
    #     if site.robots.can_fetch(USER_AGENT, site.domain):
    #         retrieve_page(site.domain)

        # retrieve_page(site_url)


        # pool = Pool(10)
        # pool.map(get_page, page_queue)
        #
        # pool.terminate()
        # pool.join()
