import re

def _transform_informations(book_title, price_of_book, qtd_stock, description_of_book):
    """This function have responsibility from transform data

    Args:
        book_title (str): title information from book where extracted with request and bs4
        price_of_book (str): price information from book where extracted with request and bs4
        qtd_stock (str): stock information from book where extracted with request and bs4
        description_of_book (str): description information from book where extracted with request and bs4

    Returns:
        str: all informations process you need for insert to database
    """
    if not book_title or not price_of_book or not qtd_stock or not description_of_book:
        return "N/A", 0.0, 0, "N/A"

    title = book_title.get_text(strip=True).capitalize() if book_title else "N/A"
    price = float(price_of_book.get_text(strip=True).replace("£", "")) if price_of_book else 0.0
    stock = re.findall(r'\d+', qtd_stock.get_text(strip=True)) if qtd_stock else []
    stock = int(stock[0]) if stock else 0
    description = description_of_book[0].strip() if description_of_book else "N/A"

    return title, price, stock, description
