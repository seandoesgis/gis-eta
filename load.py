import os
import io
import zipfile
import psycopg2
from dotenv import load_dotenv
import geopandas as gpd
from geopandas import GeoDataFrame
import pandas as pd
from sqlalchemy import create_engine
import requests
import math
import numpy as np
from urllib.parse import urlparse

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

portal = {
    "username": os.getenv("PORTAL_USERNAME"),
    "password": os.getenv("PORTAL_PASSWORD"),
    "client": os.getenv("PORTAL_CLIENT"),
    "referer": os.getenv("PORTAL_URL"),
    "expiration": int(os.getenv("PORTAL_EXPIRATION")),
    "f": os.getenv("PORTAL_F")
    }


def explode_gdf_if_multipart(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Check if the GeoDataFrame has multipart geometries.
    If so, explode them.
    """
    gdf = gdf[gdf.geometry.notnull()]

    if gdf.geom_type.str.contains('Multi').any():
        gdf = gdf.explode(index_parts=False)

    return gdf


def fetch_portal_token():
    """
    Generate a token for ArcGIS server.
    """    
    try:
        response = requests.post("https://arcgis.dvrpc.org/dvrpc/sharing/rest/generateToken", data=portal)
        response.raise_for_status()
        response_obj = response.json()
        token = response_obj.get("token")
        if not token:
            raise ValueError("Failed to retrieve token.")
        return token
    except requests.RequestException as e:
        raise SystemError(f"An error occurred while fetching the token: {e}")


def load_gis_data(dbname, target_schema, url_key, url, crs):
    """
    Loads the data from feature services into database.
    """
    print("\t -> Loading GIS data...")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    if 'opendata.arcgis.com/' in url.lower():
        print(f"\t \t -> Loading direct GeoJSON for {url_key}...")
        response = requests.get(url)
        gdf = gpd.read_file(response.content)
        gdf = gdf.to_crs(crs)
    else:
        if url.startswith("https://arcgis.dvrpc.org"):
            token = fetch_portal_token()
        else:
            token = None

        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split("/")
        base_url = url.split('?')[0]

        count_url = f"{base_url}?where=1=1&returnCountOnly=true"
        if token:
            count_url += f"&token={token}"
        count_url += "&f=json"

        count_response = requests.get(count_url)
        total_features = count_response.json().get("count")

        gdf_list = []

        total_chunks = math.ceil(total_features / 2000) # 2000 default esri record limit on feature services

        print(f"\t \t -> {url_key}...")

        for chunk in range(total_chunks):
            offset = chunk * 2000
            query_url = f"{url}&resultOffset={offset}&resultRecordCount=2000"
            if token:
                query_url += f"&token={token}"
            response = requests.get(query_url)
            data = response.json()

            chunk_gdf = gpd.GeoDataFrame.from_features(data['features'])
            gdf_list.append(chunk_gdf)

        gdf = pd.concat(gdf_list, ignore_index=True)

    if 'geometry' not in gdf.columns:
        # no geometry service
        gdf.columns = map(str.lower, gdf.columns)
        gdf.to_sql(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)
    else:
        # geometries
        gdf.columns = map(str.lower, gdf.columns)
        gdf.crs = crs
        gdf.to_postgis(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)


def download_and_load_gtfs(dbname, gtfs_url):
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

    schemas = {'google_bus', 'google_rail'}

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
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

        for file_name in os.listdir(os.path.join('gtfs', schema)):
            if file_name.endswith('.txt'):
                file_path = os.path.join('gtfs', schema, file_name)
                table_name = os.path.splitext(file_name)[0]

                df = pd.read_csv(file_path)
                df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)


def csv_table(dbname, target_schema, csv):
    """
    Loads the csv into database.
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    df = pd.read_csv(csv)
    df.columns = map(str.lower, df.columns)
    table_name = os.path.splitext(os.path.basename(csv))[0]
    print(f"Loading {table_name}.csv...\n")
    df.to_sql(table_name, con=engine, schema=target_schema, if_exists='replace', index=False)


def load_matrix(csv_path_i, csv_path_o, dbname, target_schema, table_name, minutes=45):
    """
    Find zones in matrix tables w/in 45 minutes and output to database for analysis
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    df_i = pd.read_csv(csv_path_i, index_col=0)
    df_o = pd.read_csv(csv_path_o, index_col=0)

    total_time = df_i.values + df_o.values

    within_threshold_mask = total_time <= minutes
    
    o_taz, d_taz = np.where(within_threshold_mask)
    df = pd.DataFrame({
        'o_taz': df_i.index[o_taz],
        'd_taz': df_i.columns[d_taz],
        'total_time': total_time[o_taz, d_taz]
    })
    
    df.to_sql(table_name, engine, schema=target_schema, if_exists='replace', index=False)