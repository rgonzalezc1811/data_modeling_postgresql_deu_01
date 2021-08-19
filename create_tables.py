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
import sql_queries

def create_database():
    """Creates and connects to the 'data_modeling_postgres_01' database

    This database is used to create the 'sparkifydb', then disconnects
    from 'data_modeling_postgres_01' and reconnects to the created
    database 'sparkifydb'.

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
    # Connect to the database, pay attention to the credentials!
    # TODO: Verify all the fields and insert password.
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

    except psycopg2.Error as error:
        message = traceback.format_exc()
        print("ERROR: Connection to the PostgreSQL database failed!!! :c\n")
        print(f"Error:\n{error}\n")
        print(f"Complete log error:\n{message}\n")

    # Create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb;")
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;"
    )

    # close connection to default database
    conn.close()

    # Connect to 'sparkifydb' database, becareful with the credentials!
    try:
        # TODO: Verify all the fields and insert password.
        conn = psycopg2.connect(
            user="admin",
            password="<password>",
            host="localhost",
            port="5432",
            dbname="sparkifydb"
        )
        cur = conn.cursor()

    except psycopg2.Error as error:
        message = traceback.format_exc()
        print("ERROR: Connection to the PostgreSQL database failed!!! :c\n")
        print(f"Error:\n{error}\n")
        print(f"Complete log error:\n{message}\n")

    return cur, conn

def drop_tables(cur, conn):
    """Drop all the crated tables if they exist

    The function uses the queries in 'drop_table_queries' list, the list
    is located in the 'sql_queries.py' file.

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
    sq = sql_queries.Queries()
    for query in sq.drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creat the tables if they no exist.

    Each table is created using the queries in the
    `create_table_queries` list, the list is located in the
    'sql_queries.py' file.

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
    sq = sql_queries.Queries()
    for query in sq.create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """ Pipeline execution.

    * Drop (if exists) and Create the sparkify database.
    * Establish connection with the sparkify database and get
      cursor to it.
    * Drop all the tables if they exist.
    * Create all the tables needed if they no exist.
    * Close the connection.
    """
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
