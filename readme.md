# Equity Through Access (ETA) GIS analysis

Builds a PostgreSQL database, loads necessary data, completes analysis

## Inputs
### Geography
- Blockgroup
- TAZ
### Vulnerable Populations
- Households with 1 or More People with Disability (Census ACS, blockgroup)
- Number of Households Below Poverty Line (Census ACS, blockgroup)
- People 65 or Older (Census ACS, blockgroup)
### Essential Services
- Food Stores Grocery Stores (Overture Maps)
- Health Care Facilities (Overture Maps)
- Colleges/Universities, Private/Public Schools (National Center for Education Statistics, NCES)
- Parks/Open Space (DVRPC)
- Trails (DVRPC)
- Jobs (Census LODES)
### Transit Accessibility
- AM Transit 45 minute TAZ Matrix
- Essential Services in 45 minute TAZ zones
- [ ] Walkability to transit for block groups (percentage of block group covered by transit walksheds)
- [ ] Daily Departures by TAZ


## Requirements
- PostgreSQL w/ PostGIS
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
3. Edit variables in `run.py` if needed.  Default is sufficient, but can be customized.
4. Start the process
    ```
    python run.py
    ```

## Output