from urllib.robotparser import RobotFileParser
from sqlalchemy import Column, Integer, String, TIMESTAMP, Binary, ForeignKey, LargeBinary, Table, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from hashlib import sha256
from selenium.common.exceptions import TimeoutException
from datetime import datetime
from utils import *
import threading
import requests

page_selection_lock = threading.Lock()

Base = declarative_base()


class Site(Base):
    __tablename__ = "site"

    id = Column(Integer, primary_key=True)
    domain = Column(String)
    robots_content = Column(String, default="")
    sitemap_content = Column(String, default="")

    pages = relationship("Page", back_populates="site")

    @staticmethod
    def find_or_create_site(domain, db):
        # with page_selection_lock:
        site = db.query(Site).filter(Site.domain == domain).first()
        if not site:
            site = Site(domain=domain)
            db.add(site)
            if not site.robots_content:
                site.retrieve_site_robots()
            db.commit()
        return site

    def retrieve_site_robots(self):
        url = url_normalize(self.domain + "/robots.txt")
        rp = RobotFileParser()
        rp.set_url(url)
        try:
            rp.read()
            self.robots_content = str(rp)
            if rp.site_maps():
                self.retrieve_sitemap_content(rp)
        except Exception as e:
            self.robots_content = ""
            print(f"ERROR retrieving robots.txt for {self.domain}")
            print(e)


    def retrieve_sitemap_content(self, robots):
        url = url_normalize(robots.site_maps()[0])
        try:
            response = requests.get(url, verify=False, timeout=5)
            if response.status_code == 200:
                if isinstance(response.content, str):
                    self.sitemap_content = response.content
                else:
                    self.sitemap_content = response.content.decode("utf-8")
        except Exception as ex:
            pass

    def get_robots(self):
        rp = RobotFileParser()
        if self.robots_content:
            rp.parse(self.robots_content)
        else:
            rp.allow_all = True
        return rp


link_table = Table("link", Base.metadata,
                   Column("from_page", Integer, ForeignKey("page.id", ondelete="CASCADE")),
                   Column("to_page", Integer, ForeignKey("page.id", ondelete="CASCADE")))


class Page(Base):
    __tablename__ = "page"

    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey("site.id"))
    page_type_code = Column(String, ForeignKey("page_type.code"))
    url = Column(String)
    html_content = Column(String)
    http_status_code = Column(Integer)
    accessed_time = Column(TIMESTAMP)
    checksum = Column(LargeBinary)
    canonical_link = ""
    domain = ""
    depth = Column(Integer, default=0)

    site = relationship("Site", back_populates="pages")
    images = relationship("Image", back_populates="page")
    page_data = relationship("PageData", back_populates="page")

    from_page = relationship(
        'Page', secondary=link_table,
        primaryjoin=link_table.c.to_page == id,
        secondaryjoin=link_table.c.from_page == id,
        back_populates="to_page")

    to_page = relationship(
        'Page', secondary=link_table,
        primaryjoin=link_table.c.from_page == id,
        secondaryjoin=link_table.c.to_page == id,
        back_populates="from_page")

    @staticmethod
    def find_or_create_page(url, db, depth):
        with page_selection_lock:
            existing_page = db.query(Page).filter(Page.url == url).first()
            if not existing_page:
                # Find or create the site of the page
                normalized_url = url_normalize(url)
                domain = get_domain(normalized_url)
                site = Site.find_or_create_site(domain, db)
                if not site:
                    return
                # Skip the page, if the site's robots.txt doesn't allow it
                if not site.get_robots().can_fetch(USER_AGENT, url):
                    return
                existing_page = Page(url=url, site=site, page_type_code="FRONTIER", depth=depth)
                db.add(existing_page)
                db.commit()
        return existing_page

    def retrieve_page(self, db, browser):
        url = url_normalize(self.url)
        print(f"{threading.currentThread().ident}: Crawling {url}")
        self.domain = get_domain(url)
        try:
            response = requests.head(url, verify=False, timeout=5)
        except TimeoutException as te:
            print(f"{threading.currentThread().ident}: HEAD TimeoutException on {url}")
            self.http_status_code = 408
            self.accessed_time = get_current_datetime()
            self.page_type_code = "TIMEOUT"
            return
        except Exception as e:
            print(f"{threading.currentThread().ident}: HEAD Exception on {url}")
            self.http_status_code = 408
            self.accessed_time = get_current_datetime()
            self.page_type_code = "TIMEOUT"
            return

        self.http_status_code = response.status_code
        self.accessed_time = get_current_datetime()

        if response.status_code >= 400:
            self.page_type_code = "ERROR"
            return
        elif response.status_code in [301, 302, 307, 308]:
            redirect_url = response.headers.get("Location", "")
            redirect_url = list(set(clean_urls(map(lambda x: url_normalize(self.domain + x if x.startswith("/") else x), [redirect_url]))))
            if len(redirect_url) <= 0:
                self.page_type_code = "ERROR"
                return
            redirect_url = redirect_url[0]
            print(f"{threading.currentThread().ident}: Redirect {url} -> {redirect_url}")
            redirect_page = Page.find_or_create_page(redirect_url, db, self.depth)
            if redirect_page:
                for from_pg in self.from_page:
                    if redirect_page not in from_pg.to_page:
                        from_pg.to_page.append(redirect_page)
            db.delete(self)
            return


        content_type = get_content_type(response.headers)

        self.set_page_type_code(content_type, db)

        if content_type != "text/html":
            self.page_data = [PageData(page=self, data_type_code=get_page_data_type(content_type))]
            return
        try:
            browser.get(url)

        except TimeoutException as te:
            print(f"{threading.currentThread().ident}: GET TimeoutException on page {url}")
            self.http_status_code = 408
            self.page_type_code = "TIMEOUT"
            return

        self.accessed_time = datetime.fromtimestamp(datetime.timestamp(datetime.now()))

        self.set_canonical_link(browser, db)

        # If page has a canonical link, mark it as duplicate, to remove it from frontier
        if self.canonical_link:
            self.page_type_code = "DUPLICATE"
            return

        self.html_content = self.get_html_content(browser)

        self.checksum = self.get_checksum(self.html_content)

        self.set_page_type_code(content_type, db)

        links = self.get_links(browser)

        for link in links:
            existing_page = Page.find_or_create_page(link, db, self.depth+1)
            if existing_page and existing_page not in self.to_page:
                self.to_page.append(existing_page)

        images = [link.get_attribute("src") for link in
                  browser.find_elements_by_xpath("//img[@src]")]

        self.images = [Image(page=self, filename=img if not img.startswith("data") and len(img) < 255 else "") for img in images]

    def get_links(self, browser):
        links = [link.get_attribute("href") for link in
                 browser.find_elements_by_xpath("//a[@href]")]

        links += [get_url_onclick(link.get_attribute("onclick")) for link in
                  browser.find_elements_by_xpath("//*[contains(@onclick,'location.href') or "
                                                 "contains(@onclick,'document.location)')]")]
        # prepend domain name, normalize urls, remove non *.gov, mailto links. remove duplicates
        return list(set(clean_urls(map(lambda x: url_normalize(self.domain + x if x.startswith("/") else x), links))))

    def get_checksum(self, content):
        m = sha256()
        m.update(content.encode("utf-8"))
        return m.digest()

    def get_html_content(self, browser):
        if isinstance(browser.page_source, str):
            return browser.page_source
        return browser.page_source.decode("utf-8")

    def set_canonical_link(self, browser, db):
        canonical = browser.find_elements_by_xpath("//link[@rel='canonical']")
        if canonical:
            link = url_normalize(canonical[0].get_attribute("href"))
            if len(clean_urls([link])) == 0:
                return
            if link and link != self.url:
                link = url_normalize(link)
                original_page = db.query(Page).filter(Page.url == link).first()
                if not original_page:
                    original_page = Page(url=link, page_type_code="FRONTIER")
                    db.add(original_page)
                if original_page.page_type_code == "FRONTIER":
                    original_page.retrieve_page(db, browser)
                self.canonical_link = link

    def set_page_type_code(self, content_type, db):
        self.page_type_code = "HTML" if content_type == "text/html" else "BINARY"
        if self.checksum:
            existing_page = db.query(Page).filter(and_(Page.checksum == self.checksum, Page.url != self.url)).first()
            if existing_page:
                self.page_type_code = "DUPLICATE"

    def get_domain(self):
        url = url_normalize(self.url)
        self.domain = get_domain(url)
        return self.domain


class Image(Base):
    __tablename__ = "image"

    id = Column(Integer, primary_key=True)
    page_id = Column(Integer, ForeignKey("page.id"))
    filename = Column(String)
    content_type = Column(String)
    data = Column(Binary)
    accessed_time = Column(TIMESTAMP)

    page = relationship("Page", back_populates="images")


class PageData(Base):
    __tablename__ = "page_data"

    id = Column(Integer, primary_key=True)
    page_id = Column(Integer, ForeignKey("page.id"))
    data_type_code = Column(String, ForeignKey("data_type.code"))
    data = Column(Binary)

    page = relationship("Page", back_populates="page_data")


class DataType(Base):
    __tablename__ = "data_type"

    code = Column(String, primary_key=True)


class PageType(Base):
    __tablename__ = "page_type"

    code = Column(String, primary_key=True)


class VisitedIP(Base):
    __tablename__ = "visited_ip"

    ip = Column(String, primary_key=True)
    last_visited = Column(TIMESTAMP)
