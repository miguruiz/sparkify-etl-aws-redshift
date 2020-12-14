import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Creates staging tables. Copies data from s3 into redshift without any transformation.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transforms and inserts data from the staging tables into the final ones.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the enviroment specified in the conf file, and executesload_staging_tables and insert_tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    ENDPOINT           = config.get("OTHER","ENDPOINT")
    DB                 = config.get("CLUSTER","DB_NAME")
    DB_USER            = config.get("CLUSTER","DB_USER")
    DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    PORT               = config.get("CLUSTER","DB_PORT")

    conn = psycopg2.connect(f"host={ENDPOINT} dbname={DB} user={DB_USER} password={DB_PASSWORD} port={PORT}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()