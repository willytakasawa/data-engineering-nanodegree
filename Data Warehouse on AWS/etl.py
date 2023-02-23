import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load staging tables.

    Keyword arguments:
    cur -- cursor of psycopg2
    conn -- connection of psycopg2
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Load dimension and fact tables.

    Keyword arguments:
    cur -- cursor of psycopg2
    conn -- connection of psycopg2
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Read config file
    - Connects to database
    - Loads staging tables
    - Insert data into dimension and fact tables
    - Close connection
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
