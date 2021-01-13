import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all table found in drop_table_queries list
    
    Arguments:
    a. {object} -- The cursor point to the connected database schema
    b. {object} -- The established Redshift connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all table found in create_table_queries (list of a staging, fact
    and dimension tables)
    
    Arguments:
    a. {object} -- The cursor point to the connected database schema
    b. {object} -- The established Redshift connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Creates a connection
    - Get the cursor to the schema
    - Drops existing tables and
    - Create required tables
    - Close connection
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Done")


if __name__ == "__main__":
    main()