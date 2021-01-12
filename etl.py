import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def select_statement(cur, conn):
        query = "SELECT DISTINCT user_id, start_time, first_name, last_name, gender, level FROM songplays, users WHERE user_id IS NOT NULL;"
        results = cur.execute(query)
        conn.commit()
        return results

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    select_statement(curr,conn)

    conn.close()


if __name__ == "__main__":
    main()