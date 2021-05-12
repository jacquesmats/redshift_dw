import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """COPY S3 data to staging tables: staging_events and staging_songs"""
    
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transform and INSERT data from staging tables for fact and dimensions tables"""
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("## Connecting to Redshift")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("## Copying S3 data to staging tables")
    load_staging_tables(cur, conn)
    
    print("## Transforming data and loading in analytics tables")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()