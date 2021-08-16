# -*- coding: utf-8 -*-
"""Script to create and drop tables.

This process make sure that the tables can be created successfully and
helps to debug queries if needed.

Mantaniner: Rolando Gonzalez
Version: 1.0.0
"""
# Standard library imports
import traceback

# Third-party imports
import psycopg2

# Propietary imports
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """Creates and connects to the 'sparkifydb' database

    This database represents the Sparkify database to handle JSON data.

    Returns
    -------
    cur : Instance
        description -> psycopg2 instance 'sparkifydb' cursor
        format -> No apply
        options -> No apply

    conn : Instance
        description -> psycopg2 instance 'sparkifydb' connection
        format -> No apply
        options -> No apply
    """
    # Connect to the database
    try:
        conn = psycopg2.connect(
            user="admin",
            password="<password>",
            host="localhost", 
            port="5432",
            dbname="data_modeling_postgres_01"
        )
        conn.set_session(autocommit=True)
        cur = conn.cursor()

    except psycopg2.Error as e:
        message = traceback.format_exc()
        print("ERROR: Connection to the PostgreSQL database failed!!! :c\n")
        print(f"Error:\n{e}\n")
        print(f"Complete log error:\n{message}\n")

    # Create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb;")
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;"
    )

    # close connection to default database
    conn.close()
    
    # Connect to 'sparkifydb' database
    try:
        conn = psycopg2.connect(
            user="admin",
            password="98573Hgte", # <password>
            host="localhost", 
            port="5432",
            dbname="sparkifydb"
        )
        cur = conn.cursor()

    except psycopg2.Error as e:
        message = traceback.format_exc()
        print("ERROR: Connection to the PostgreSQL database failed!!! :c\n")
        print(f"Error:\n{e}\n")
        print(f"Complete log error:\n{message}\n")

    return cur, conn

def drop_tables(cur, conn):
    """Drop all the crated tables.
    
    The function uses the queries in 'drop_table_queries' list.

    Parameters
    ----------
    cur : Instance
        description -> psycopg2 instance 'sparkifydb' cursor
        format -> No apply
        options -> No apply

    conn : Instance
        description -> psycopg2 instance 'sparkifydb' connection
        format -> No apply
        options -> No apply
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creat the tables.

    Each table is created using the queries in `create_table_queries`
    list.

    Parameters
    ----------
    cur : Instance
        description -> psycopg2 instance 'sparkifydb' cursor
        format -> No apply
        options -> No apply

    conn : Instance
        description -> psycopg2 instance 'sparkifydb' connection
        format -> No apply
        options -> No apply
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """ Pipeline execution.

    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets
      cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
