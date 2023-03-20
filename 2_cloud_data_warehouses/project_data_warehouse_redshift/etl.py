import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load tables in the staging area of the Data Warehouse"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Data loaded into staging table.")
    print("Data succesfully loaded into staging tables.")


def insert_tables(cur, conn):
    """Insert final tables in the Data Warehouse"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Data loaded into final table.")
    print("Data succesfully inserted into final tables.")


def main():
    """Main ETL function"""
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
