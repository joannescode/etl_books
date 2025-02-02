import sqlite3
from src.logger import get_logger
import pandas as pd

register = get_logger()


def connection_to_database(database):
    """Connect to database, use address or name file for this

    Args:
        database (str): name file or address for your database

    Returns:
        sql object (con and cursor): con for interact with database and cursor for recive con.cursor for use queries
    """
    try:
        con = sqlite3.connect(database=database)
        cursor = con.cursor()
        register.info("Connection to database OK!")
        return con, cursor
    except Exception as e:
        register.error(f"Exception in fuction 'connection_to_database': {e}")


def create_table_for_books(cursor, con):
    """This function serves for create a new table where your insert values

    Args:
        cursor (sql object): cursor where contains con object for use queries
        con (sql object): con for interact with database
    """
    try:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            price REAL NOT NULL,
            availability INTEGER NOT NULL
        );"""
        )
        con.commit()

    except Exception as e:
        register.error(f"Exception in fuction 'insert_values': {e}")


def insert_values(cursor, con, title, price, stock, description):
    """Use to insert values extract from books to scrape

    Args:
        cursor (sql object): pass cursor from previously function where you use for insert values in database
        con (sql object): pass con from previously function where you use for commit values in database
        title (str): title information from book collected
        price (str): price information from book collected
        stock (str): stock information from book collected
        description (str): description information from book collected
    """
    try:
        cursor.execute(
            """INSERT INTO books(title,description,price,availability) VALUES(?,?,?,?)""",
            (title, description, price, stock),
        )
        con.commit()
        register.info(f"Insert values from book: {title} OK!")

    except Exception as e:
        register.error(f"Exception in fuction 'insert_values': {e}")


def output_db_csv(database):
    """This function return a csv file where contains informations from database

    Args:
        database (str): name file or address for your database
    """
    try:
        con = sqlite3.connect(database)
        query = """SELECT * FROM books"""
        df = pd.read_sql(query, con)
        df.to_csv("registers.csv", index=False)
    except Exception as e:
        register.error(f"Exception in fuction 'output_db_csv': {e}")
    finally:
        if con:
            con.close()
