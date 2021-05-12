import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """DROP existing tables using drop_table_queries list, in order to configure it from scracth"""

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """CREATE tables needed for Sparkfy: Staging and Analytics tables."""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Main program"""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("## Connecting to Redshift")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("## Dropping existing tables...")
    drop_tables(cur, conn)
    
    print("## Creating tables...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()