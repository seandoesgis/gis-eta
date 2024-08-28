import os
import pandas as pd
import requests
import zipfile
import io
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")


def load_acs_data(variables, year, state_county_pairs, dbname, schema):
    """
    Get census data
    """
    print("\t -> Loading ACS data table...")
    base_url = f"https://api.census.gov/data/{year}/acs/acs5"
    all_data = []

    for state, counties in state_county_pairs:
        for county in counties:
            params = {
                "get": ",".join(variables),
                "for": "block group:*",
                "in": f"state:{state} county:{county}"
            }

            response = requests.get(base_url, params=params)
            response.raise_for_status()

            data = response.json()
            df = pd.DataFrame(data[1:], columns=data[0])
            all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.columns = map(str.lower, combined_df.columns)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    combined_df.to_sql("acs_data", engine, schema=schema, if_exists='replace', index=False)


def load_lodes_data(dbname, schema):
    """
    Get LODES data for NJ and PA job totals
    """
    print("\t -> Loading LODES job data...")
    lodes_states = ['nj', 'pa']
    combined_df = pd.DataFrame()

    for state in lodes_states:
        url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/wac/{state}_wac_S000_JT00_2021.csv.gz"
        response = requests.get(url)
        zipped_file = io.BytesIO(response.content)
        state_df = pd.read_csv(zipped_file, compression='gzip')

        combined_df = pd.concat([combined_df, state_df], ignore_index=True)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    combined_df.columns = map(str.lower, combined_df.columns)
    combined_df.to_sql("lodes_data", engine, schema=schema, if_exists='replace', index=False)

