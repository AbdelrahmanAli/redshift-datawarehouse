import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 bucket to staging tables"""
    print("-------------------------------")
    print("Loading Staging tables from S3:")
    print("-------------------------------")
    for query in copy_table_queries:
        print("Executing Query:"+query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into Star Schema tables"""
    print("-------------------------------")
    print("Insert into star schema tables:")
    print("-------------------------------")
    for query in insert_table_queries:
        print("Executing Query:"+query)
        cur.execute(query)
        conn.commit()


def main():
    """Start the ETL procedure"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()