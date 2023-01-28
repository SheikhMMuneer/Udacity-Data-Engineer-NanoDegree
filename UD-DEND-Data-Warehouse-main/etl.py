import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function is used to trigger the extract-process
                 of the data from the json-files to the staging tables
                 
    Arguments:
        cur: the cursor object
        conn: object of the connection to the database
        
    Returns:
        None
    """
    try:
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)


def insert_tables(cur, conn):
    """
    Description: This function triggers the transform and load process.
    
    Arguments:
        cur: the cursor object
        conn: object of the connection to the database
        
    Returns:
        None
    """
    try:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)


def main():
    """
    Description: This main function connects to the database and provides the cursor.
                 It also triggers the functions staging_tables and insert_tables.
    
    Arguments:
        None
        
    Returns:
        None
    """
    try:
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        conn.close()
    except psycopg2.Error as e:
        print(e)
    

if __name__ == "__main__":
    main()