import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    loop over the list of copy table queries and
    execute sql query to copy data from S3 buckets (provided in dwh.cfg)
    to staging tables in Redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    loop over the list of insert table queries and
    execute sql query to insert data into final data tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main function to connect to Redshift
    and perform data copy and data insertion
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config['CLUSTER']['HOST'], config['CLUSTER']['DB_NAME'], config['CLUSTER']['DB_USER'], config['CLUSTER']['DB_PASSWORD'], config['CLUSTER']['DB_PORT']))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()