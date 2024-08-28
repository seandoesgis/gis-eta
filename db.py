import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import re

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")


def create_database(dbname):
    """
    Creates a PostgreSQL db.
    """
    print("\t -> Creating database...")
    pgconn = psycopg2.connect(
        host=host, port=port, database=database, user=user, password=password
    )
    pgconn.autocommit = True
    cur = pgconn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {dbname};")    
    cur.execute(f"CREATE DATABASE {dbname};")
    cur.close()
    pgconn.close()


def create_schemas(dbname, schemas):
    """
    Creates PostgreSQL schemas with the given names in the db.
    """
    print("\t -> Creating schemas...")
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = conn.cursor()
    conn.autocommit = True

    for schema in schemas:
        cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname='{schema}'")
        schema_exists = bool(cur.rowcount)
        if not schema_exists:
            cur.execute(f"CREATE SCHEMA {schema};")
    cur.close()
    conn.close()


def create_extensions(dbname):
    """
    Creates the POSTGIS/PGROUTING extension in the db.
    """
    print("\t -> Activating spatial extensions...")
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = conn.cursor()
    conn.autocommit = True

    cur.execute("SELECT 1 FROM pg_extension WHERE extname='postgis'")
    postgis_extension_exists = bool(cur.rowcount)

    if not postgis_extension_exists:
        cur.execute("CREATE EXTENSION POSTGIS;")

    cur.execute("SELECT 1 FROM pg_extension WHERE extname='pgrouting'")
    pgrouting_extension_exists = bool(cur.rowcount)

    if not pgrouting_extension_exists:
        cur.execute("CREATE EXTENSION PGROUTING;")

    cur.close()
    conn.close()


def do_analysis(dbname, sql):
    """
    Executes the analysis sql.  Messy but working....
    """
    print("\t -> Running SQL...")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    with open(sql, 'r') as sql_file:
        sql_contents = sql_file.read()

    transaction_blocks = re.split(r'COMMIT;\s*\n', sql_contents)

    with engine.connect() as connection:
        try:
            for transaction_block in transaction_blocks:
                transaction_block = transaction_block.strip()
                if not transaction_block:
                    continue
                
                comment_matches = re.findall(r'--(.*)', transaction_block)
                if comment_matches:
                    for comment in comment_matches:
                        print(f"\t \t -> {comment.strip()}\n")

                try:
                    connection.execute(text(transaction_block))
                except Exception as e:
                    print("Error message:", str(e))
                    break
                try:
                    connection.execute(text('commit;'))
                except Exception as e:
                    print("Error during commit:", str(e))
                    break
        except:
            raise
