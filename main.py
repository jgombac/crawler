from db_classes import *
from utils import *
from url_normalize import url_normalize
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

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
    db.close()

