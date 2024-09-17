# DVRPC Equity Through Access (ETA) GIS analysis

The Equity Through Access (ETA) project is DVRPC’s update of the region’s Coordinated Human Services Transportation Plan (CHSTP). ETA seeks to improve economic and social opportunity in the region by expanding access to essential services for vulnerable populations - those who are more critically impacted by barriers and gaps in infrastructure, service coordination, and policies. Vulnerable populations are individuals who are low income, seniors, physically disabled, mentally disabled, and more likely to be transit dependent than the general population. Essential services are defined as destinations needed to meet a standard quality of life and include places of employment, grocery stores, schools, medical facilities, recreation/open space areas, senior centers, and centers for the developmentally disabled. This project responds to the changing CHSTP funding landscape and looks for new ways to promote accessible, affordable, and safe mobility.

This repo builds a PostgreSQL database, loads necessary data, and completes the ETA GIS data analysis for the DVRPC region.

## Inputs
### Geography
- 2020 Blockgroup
- TAZ
### Vulnerable Populations
- Households with 1 or More People with Disability (Census ACS, blockgroup)
- Number of Households Below Poverty Line (Census ACS, blockgroup)
- People 65 or Older (Census ACS, blockgroup)
### Essential Services
- Senior Services and Care (Overture Maps)
- Food Stores Grocery Stores (Overture Maps)
- Health Care Facilities (Overture Maps)
- Colleges/Universities, Private/Public Schools (National Center for Education Statistics, NCES)
- Parks/Open Space (DVRPC)
- Trails (DVRPC)
- Jobs (Census LODES)
### Transit Accessibility
- AM Transit 45 minute TAZ Matrix (DVRPC travel model)
- Essential Services in 45 minute TAZ zones (Overture Maps, DVRPC travel model)
- Walkability to transit percentage of block group covered by transit 15min rail and 5 minute bus walksheds (DVRPC pedestrian network, GTFS - SEPTA, NJTRANSIT, PATCO)
- Daily Departures by TAZ (GTFS - SEPTA, NJTRANSIT, PATCO)


## Requirements
- PostgreSQL w/ PostGIS, pgRouting
- Python 3.11
- .env with PostgreSQL credentials and DVRPC ArcGIS Portal credentials
- Transit travel time TAZ matrix tables (csv)

### Run
1. Clone the repo
    ``` cmd
    git clone https://github.com/dvrpc/gis-eta.git
    ```
2. Create a Python virtual environment with dependencies

    Working in the repo directory from your terminal:

    - create new venv
    ```cmd
    python -m venv venv
    ```
    - activate venv
    ```
    .\venv\scripts\activate
    ```
    - install requirements
    ```
    pip install -r requirements.txt
    ```
    - copy .env_sample and rename to .env
    ```
    copy .env_sample .env
    ```
    - edit .env environmental variables in VSCode and provide PostgreSQL/ArcGIS Portal credentials
3. Edit variables in `run.py` as needed.  

    **Variables and field names might also need to be adjusted/updated in `analysis.sql` if reran.**  An example of this would be service calendar dates for GTFS (lines 398, 407, 415, 424, 433, 443).  Data structure could change on some inputs in the future as well.

4. Start the process
    ```
    python run.py
    ```

## Output

All outputs are saved to the `output` schema in the database.  Scoring for each category is saved:

- output.vul_pop_rank
- output.es_rank
- output.access_gap_rank
- output.transit_rank

and the total ETA scoring by blockgroup...

- output.output

Detailed metadata can be found here **insert metadata url ;)**