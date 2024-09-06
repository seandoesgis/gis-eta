import os
import io
import zipfile
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import requests
import math
from urllib.parse import urlparse

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")


def download_and_load_septagtfs(dbname, gtfs_url):
    """
    downloads, extracts, loads septa gtfs into the db
    """
    print("\t -> Loading SEPTA GTFS data...")
    response = requests.get(gtfs_url)
    zip_content = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
        zip_ref.extractall('gtfs')

    zip_files = [file for file in os.listdir('gtfs') if file.endswith(".zip")]
    for zip in zip_files:
        path = os.path.join('gtfs', zip)
        file_name = os.path.splitext(path)[0] 
        os.mkdir(file_name) 
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(file_name))

    folder_to_schema = {
        'google_bus': 'septa_bus',
        'google_rail': 'septa_rail'
    }

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = conn.cursor()
    conn.autocommit = True

    for folder, schema in folder_to_schema.items():
        cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname='{schema}'")
        schema_exists = bool(cur.rowcount)
        if not schema_exists:
            cur.execute(f"CREATE SCHEMA {schema};")

        folder_path = os.path.join('gtfs', folder)
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.txt'):
                file_path = os.path.join(folder_path, file_name)
                table_name = os.path.splitext(file_name)[0]

                df = pd.read_csv(file_path)
                df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)

    cur.close()
    conn.close()


def download_and_load_njtgtfs(dbname, gtfs_url):
    """
    downloads, extracts, loads njtransit gtfs into the db
    """
    print("\t -> Loading NJT GTFS data...")
    gtfs_type = gtfs_url.rsplit('/', 1)[-1]
    mode = gtfs_type.split('_')[0]
    response = requests.get(gtfs_url)
    zip_content = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
        zip_ref.extractall(f'./gtfs/njtransit_{mode}')
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = conn.cursor()
    conn.autocommit = True

    schema = f'njtransit_{mode}'

    cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname='{schema}'")
    schema_exists = bool(cur.rowcount)
    if not schema_exists:
        cur.execute(f"CREATE SCHEMA {schema};")

    for file_name in os.listdir(os.path.join('gtfs', schema)):
        if file_name.endswith('.txt'):
            file_path = os.path.join('gtfs', schema, file_name)
            table_name = os.path.splitext(file_name)[0]

            df = pd.read_csv(file_path)
            df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)


def download_and_load_patcogtfs(dbname, gtfs_url):
    """
    downloads, extracts, loads patco gtfs into the db
    """
    print("\t -> Loading PATCO GTFS data...")
    response = requests.get(gtfs_url)
    zip_content = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
        zip_ref.extractall(f'./gtfs/patco')
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    conn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = conn.cursor()
    conn.autocommit = True

    schema = 'patco'

    cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname='{schema}'")
    schema_exists = bool(cur.rowcount)
    if not schema_exists:
        cur.execute(f"CREATE SCHEMA {schema};")

    for file_name in os.listdir(os.path.join('gtfs', schema)):
        if file_name.endswith('.txt'):
            file_path = os.path.join('gtfs', schema, file_name)
            table_name = os.path.splitext(file_name)[0]

            df = pd.read_csv(file_path)
            df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)