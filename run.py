import db
import census
import load
import os
import json
import time

start_time = time.time()

dbname = "eta"
sql = "sql/analysis.sql"
schemas = ["input", "output"]
gis_sources = "source/data_sources.json"
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


# db.create_database(dbname)

# db.create_schemas(dbname, schemas)

# db.create_extensions(dbname)

# census.load_acs_data(acs_variables, acs_year, acs_state_county_pairs, dbname, schemas[0])

# census.load_lodes_data(dbname, schemas[0])

# with open(gis_sources, 'r') as config_file:
#     urls_config = json.load(config_file)
# urls = urls_config['urls']
# for url_key, url_value in urls.items():
#     load.load_gis_data(dbname, schemas[0], url_key, url_value, crs)

# load.load_matrix('source/AM_matrix_i_put.csv', 'source/AM_matrix_o_put.csv', dbname, schemas[0], 'matrix_45min')

db.do_analysis(dbname, sql)

end_time = time.time()
duration = end_time - start_time
hours = int(duration // 3600)
minutes = int((duration % 3600) // 60)
seconds = duration % 60

print(f"Script duration: {hours} hours, {minutes} minutes, {seconds:.2f} seconds")
