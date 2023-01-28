import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function is used to drop the tables
                 defined in the array 'drop_table_queries'
    
    Arguments:
        cur: the cursor object
        conn: object of the connection to the database
        
    Returns:
        None
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)


def create_tables(cur, conn):
    """
    Description: This function is used to create the tables
                 defined in the array 'create_table_queries'
    
    Arguments:
        cur: the cursor object
        conn: object of the connection to the database
        
    Returns:
        None
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)


def main():
    """
    Description: This main function connects to the database and provides the cursor.
                 It also triggers dropping and creating of the tables.
    
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

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
    except psycopg2.Error as e:
        print(e)


if __name__ == "__main__":
    main()