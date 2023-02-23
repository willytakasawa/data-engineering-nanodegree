import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables if exists.

    Keyword arguments:
    cur -- cursor of psycopg2
    conn -- connection of psycopg2
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all needed tables.

    Keyword arguments:
    cur -- cursor of psycopg2
    conn -- connection of psycopg2
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Read config file
    - Connects to database
    - Drop tables
    - Create tables
    - Close connection
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
