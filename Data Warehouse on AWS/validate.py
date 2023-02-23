import configparser
import logging
from tabulate import tabulate

import psycopg2


tables = ['fct_songplays', 'dim_users', 'dim_songs', 'dim_artists', 'dim_time']


def select_queries(cur, conn):
    """Function to check data from all tables.

    Keyword arguments:
    cur -- cursor of psycopg2
    conn -- connection of psycopg2
    """

    try:
        for table in tables:
            cur.execute("SELECT * FROM {} LIMIT 5".format(table))
            conn.commit()
            logging.info("SELECT * FROM {} \n {}".format(table, tabulate(cur.fetchall())))
    except Exception as e:
        logging.exception(e)


def count_queries(cur, conn):
    """Function to count data from all tables.

    Keyword arguments:
    cur -- cursor of psycopg2
    conn -- connection of psycopg2
    """
    
    try:
        for table in tables:
            cur.execute("SELECT COUNT(*) FROM {}".format(table))
            conn.commit()
            logging.info("COUNT TABLE {}: {}".format(table, cur.fetchall()))
    except Exception as e:
        logging.exception(e)


def main():
    """
    - Configure the log file
    - Read config file
    - Connects to database
    - Do SELECT query in all tables
    - Do COUNT query in all tables
    - Close connection
    """

    logging.basicConfig(
        level=logging.INFO,
        filemode='w+',
        filename='validate.log',
        format='%(asctime)s %(levelname)-8s %(message)s'
    )

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    select_queries(cur, conn)
    count_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()