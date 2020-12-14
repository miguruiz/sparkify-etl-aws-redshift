import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables if exist.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create stagging and final tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the cluster in the confifuration file, and execute drop_tables and create_tables.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()