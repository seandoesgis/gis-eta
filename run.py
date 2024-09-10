import os
import db
import load
import json
import gtfs
import time
import census
import walkshed
from concurrent.futures import ThreadPoolExecutor, as_completed

start_time = time.time()

dbname = "eta"
sql = "sql/analysis.sql"
schemas = ["input", "network", "output"]
data_sources = "source/data_sources.json"
crs = "EPSG:26918"

acs_variables = [
    "B11001_001E",  # Total Number of Households
    "B01003_001E",  # Total Number of People
    "B22010_006E",  # Households with 1 or more people with a disability that received food stamps/SNAP
    "B22010_003E",  # Households with 1 or more people with a disability that did not receive food stamps/SNAP
    "B17017_002E",  # Poverty Status in the Past 12 Months by Household Type
    "B01001_020E", "B01001_021E", "B01001_022E", "B01001_023E", "B01001_024E", "B01001_025E",  # People 65 or Older (male)
    "B01001_044E", "B01001_045E", "B01001_046E", "B01001_047E", "B01001_048E", "B01001_049E"   # People 65 or Older (female)
]
acs_year = 2022
acs_state_county_pairs = [
    ("34", ["005", "007", "015", "021"]),
    ("42", ["017", "029", "045", "091", "101"])   
]

db.create_database(dbname)
db.create_schemas(dbname, schemas)
db.create_extensions(dbname)

census.load_acs_data(acs_variables, acs_year, acs_state_county_pairs, dbname, schemas[0])
census.load_lodes_data(dbname, schemas[0])

with open(data_sources, 'r') as f:
    urls = json.load(f)
gis_urls = urls['gis_urls']
for url_key, url_value in gis_urls.items():
    load.load_gis_data(dbname, schemas[0], url_key, url_value, crs)

gtfs.download_and_load_septagtfs(dbname, urls['gtfs_urls']['septa'])
for url in urls['gtfs_urls']['nj_transit']:
    gtfs.download_and_load_njtgtfs(dbname, url)
gtfs.download_and_load_patcogtfs(dbname, urls['gtfs_urls']['patco'])

load.load_matrix('source/AM_matrix_i_put.csv', 'source/AM_matrix_o_put.csv', dbname, schemas[0], 'matrix_45min')

db.do_analysis(dbname, sql)

pois = walkshed.get_transit_poi(dbname)
with ThreadPoolExecutor() as executor: # parallel process batch of pois (may need to adjust max_workers for hardware)
    future_to_poi = {executor.submit(walkshed.process_transit_poi, poi, dbname): poi for poi in pois}
    for future in as_completed(future_to_poi):
        poi = future_to_poi[future]
        try:
            future.result()
        except Exception as e:
            print(f"Error processing POI {poi['id']}: {e}") # sometimes pg can limit connections and such causing errors
walkshed.polys(dbname)

end_time = time.time()
duration = end_time - start_time
hours = int(duration // 3600)
minutes = int((duration % 3600) // 60)
seconds = duration % 60

print(f"Script duration: {hours} hours, {minutes} minutes, {seconds:.2f} seconds")
