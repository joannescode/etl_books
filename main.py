from src.extract import scrape_all_pages, scrape_all_address_books, parser_books
from src.load import (
    connection_to_database,
    create_table_for_books,
    insert_values,
    output_db_csv,
)
import requests

URL = "https://books.toscrape.com"
DATABASE = "database.sqlite3"

if __name__ == "__main__":
    try:
        with requests.Session() as session:

            visited_pages = scrape_all_pages(base_url=URL, session=session)

            product_address = scrape_all_address_books(
                base_url=URL, visited_pages=visited_pages, session=session
            )

            information = parser_books(
                base_url=URL, product_address=product_address, session=session
            )

            con, cursor = connection_to_database(database=DATABASE)
            create_table_for_books(cursor=cursor, con=con)

            num_books = len(information["title"])
            for i in range(num_books):
                insert_values(
                    cursor=cursor,
                    con=con,
                    title=information["title"][i],
                    price=information["price"][i],
                    stock=information["stock"][i],
                    description=information["description"][i],
                )

            output_db_csv(database=DATABASE)

    finally:
        if con:
            con.close()
        if session:
            session.close()
