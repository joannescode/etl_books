import requests
from src.logger import get_logger
from bs4 import BeautifulSoup
from lxml import html
import re
import concurrent.futures
from src.transform import _transform_informations

register = get_logger()


def _request_page(base_url: str, session, concat_url=None):
    """Private function for page request, create session and extract content page

    Args:
        base_url (str): address from page extract
        session (request object): session object created from with request
        concat_url (str, optional): address for concat base url page. Defaults to None.

    Returns:
        content: content from page requests for parser and extract informations
    """
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    }
    address = base_url + concat_url if concat_url else base_url
    try:
        r = session.get(url=address, timeout=5, headers=HEADERS)
        if r.status_code == 200:
            register.info(f"Request OK, status code: {r.status_code}, URL: {address}")
            return r.content
        else:
            register.warning(f"Bad Request: {r.status_code}, URL: {address}")
            return None
    except requests.RequestException as e:
        register.error(f"Request failed: {e}, URL: {address}")
        return None


def _collect_pages(content):
    """Private fuction for collect all pages from navigation with content page

    Args:
        content (object): content from page requests for parser and extract informations

    Returns:
        list: this list contains all address founds with parser for next pages
    """
    soup = BeautifulSoup(content, "html.parser")
    address_found = []
    pattern = r"page-(\d+)\.html"

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "previous" not in href and re.search(pattern, href):
            href_formated = href.replace("catalogue/", "")
            if "page-1.html" in href_formated:
                continue
            else:
                address_found.append(href_formated)

    return address_found


def scrape_all_pages(base_url, session, sub_path="/catalogue/"):
    """Function for using previously private functions where we management and collected all next pages

    Args:
        base_url (str):  address from page extract
        session (request object): session object created from with request
        sub_path (str, optional): address for concat base url page. Defaults to "/catalogue/".

    Returns:
        set: this set contains all pages visited in site
    """
    try:
        visited_pages = set()
        to_visit = [""]
        while to_visit:
            current_page = to_visit.pop(0)
            if current_page in visited_pages:
                continue

            url_base = base_url if current_page == "" else base_url + sub_path
            register.info(f"Current page is: {current_page}")

            content = _request_page(
                base_url=url_base, session=session, concat_url=current_page
            )
            if content is None:
                continue

            visited_pages.add(current_page)

            new_pages = _collect_pages(content=content)
            for page in new_pages:
                if page not in visited_pages and page not in to_visit:
                    to_visit.append(page)

        register.info(f"Scraping completed. Pages visited: {len(visited_pages)}")
        return visited_pages

    except Exception as e:
        register.error(f"Exception in fuction 'scrape_all_pages': {e}")
        if session:
            register.info("Session close and finished.")
            session.close()


def scrape_all_address_books(base_url, visited_pages, session):
    """Function for scrape address books product found

    Args:
        base_url (str):  address from page extract
        visited_pages (set): this set contains all pages visited in site
        session (request object): session object created from with request

    Returns:
        set: this set contains all address found for books
    """
    try:
        product_address = set()

        for url in visited_pages:
            content = _request_page(
                base_url=base_url,
                session=session,
                concat_url=f"/catalogue/{url}" if url else None,
            )
            if content is None:
                continue

            soup = BeautifulSoup(content, "html.parser")
            products_books = soup.find_all(class_="image_container")

            for product in products_books:
                address = product.find("a", href=True)
                product_address.add(address.get("href"))

        register.info(
            f"Scraping completed. Books address collected: {len(product_address)}"
        )
        return product_address

    except Exception as e:
        register.error(f"Exception in fuction 'scrape_all_address_books': {e}")
        if session:
            register.info("Session close and finished.")
            session.close()


def parser_books(base_url, product_address, session):
    """Function for parser and extract informations our need from books

    Args:
        base_url (str):  address from page extract
        product_address (set): this set contains all address found for books
        session (request object): session object created from with request

    Returns:
        dict: contains all informations our needed from books
    """
    information = {"title": [], "price": [], "stock": [], "description": []}

    def parse_url(url):
        try:
            url = url.replace("catalogue/", "") if "catalogue" in url else url
            content = _request_page(
                base_url=base_url, session=session, concat_url=f"/catalogue/{url}"
            )
            if content is None:
                return

            soup = BeautifulSoup(content, "html.parser")
            tree = html.fromstring(content)

            book_title = soup.find("h1")
            price_of_book = soup.find(class_="price_color")
            qtd_stock = soup.find("p", class_="instock availability")
            description_of_book = tree.xpath(
                '//*[@id="content_inner"]/article/p/text()'
            )

            title, price, stock, description = _transform_informations(
                book_title=book_title,
                price_of_book=price_of_book,
                qtd_stock=qtd_stock,
                description_of_book=description_of_book,
            )

            if title == "N/A" or price == 0.0 or stock == 0 or description == "N/A":
                register.warning(f"Warning: Invalid data for URL {url}")

            information["title"].append(title)
            information["price"].append(price)
            information["stock"].append(stock)
            information["description"].append(description)

        except Exception as e:
            register.error(f"Exception in function 'parse_url' for URL {url}: {e}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(parse_url, product_address)

    return information
