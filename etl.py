import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads JSON data from S3 in taging tables in copy_table_queries list
    
    Arguments:
    a. {object} -- The cursor point to the connected database schema
    b. {object} -- The established Redshift connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Populates data from staging tables into the respective fact and dimension tables
    
    Arguments:
    a. {object} -- The cursor point to the connected database schema
    b. {object} -- The established Redshift connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def select_statement(cur, conn):
    """
    Sample use cases to evalute the implementation
    
    Arguments:
    a. {object} -- The cursor point to the connected database schema
    b. {object} -- The established Redshift connection object
    """
    query1 = ("""SELECT COUNT(*) FROM staging_events;""")
    query2 = "SELECT COUNT(*) FROM staging_songs;"
    query3 = "SELECT COUNT(*) FROM songplays;"
    query4 = "SELECT COUNT(*) FROM artists;"
    query5 = "SELECT COUNT(*) FROM songs;"
    query6 = "SELECT COUNT(*) FROM users;"
    query7 = "SELECT COUNT(*) FROM time;"
    query8 = "SELECT DISTINCT u.user_id, u.first_name, u.last_name, u.gender, u.level, s.start_time FROM users u JOIN songplays s ON (u.user_id = s.user_id) WHERE u.user_id IS NOT NULL;"
    cur.execute(query1)
    print(cur.fetchall())
    cur.execute(query2)
    print(cur.fetchall())
    cur.execute(query3)
    print(cur.fetchall())
    cur.execute(query4)
    print(cur.fetchall())
    cur.execute(query5)
    print(cur.fetchall())
    cur.execute(query6)
    print(cur.fetchall())
    cur.execute(query7)
    print(cur.fetchall())
    cur.execute(query8)
    print(cur.fetchall())
    conn.commit()

def main():
    """
    - Creates a connection
    - Get the cursor to the schema
    - Load staging tables
    - Insert into schema from staging tables and
    - Test implementation
    - Close connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    select_statement(cur,conn)

    conn.close()


if __name__ == "__main__":
    main()