from datetime import datetime

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from seleniumrequests import PhantomJS, Firefox
from urllib.parse import urlparse
from url_normalize import url_normalize
import os
USER_AGENT = "fri-ieps-rmj"


def get_browser():
    # caps = DesiredCapabilities.PHANTOMJS
    # caps["phantomjs.page.settings.userAgent"] = USER_AGENT
    # caps["pageLoadStrategy"] = "eager"
    # browser = PhantomJS(desired_capabilities=caps, service_args=['--ignore-ssl-errors=true'], service_log_path=os.path.devnull)
    # browser.set_page_load_timeout(5)

    caps = DesiredCapabilities().FIREFOX
    options = FirefoxOptions()
    options.add_argument("--headless")
    caps["pageLoadStrategy"] = "eager"  # interactive
    browser = Firefox(desired_capabilities=caps, options=options)
    browser.set_page_load_timeout(5)
    return browser


def get_content_type(headers):
    return headers["Content-Type"].split(";")[0]


def clean_urls(urls):
    return list(filter(lambda url: ("gov.si" in url or url.startswith("/")) and not url.startswith("mailto"), urls))


def get_url_onclick(attribute):
    if "location.href=" in attribute:
        return attribute.split("location.href=")[1].replace("'", "")
    elif "document.location=" in attribute:
        return attribute.split("document.location=")[1].replace("'", "")
    return ""


PAGE_DATA_TYPES = {
    "text/html": "HTML",
    "application/pdf": "PDF",
    "application/msword": "DOC",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "DOCX",
    "application/vnd.ms-powerpoint": "PPT",
    "application/vnd.openxmlformats-officedocument.presentationml.presentatio": "PPTX"
}


def get_page_data_type(content_type):
    return PAGE_DATA_TYPES.get(content_type, "Other")


def get_domain(url):
    return urlparse(url_normalize(url)).netloc


def get_current_timestamp():
    return datetime.timestamp(datetime.now())


def get_current_datetime():
    return datetime.fromtimestamp(get_current_timestamp())


def date_to_timestamp(date):
    return datetime.timestamp(date)


def timestamp_to_date(ts):
    return datetime.fromtimestamp(ts)
