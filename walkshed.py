from sqlalchemy import create_engine, text, NullPool
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

host = os.getenv("HOST")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

# distance = speed×time
# walking speed = 1.4 m/s
# walk rail = 15 minutes -> 900 seconds
# rail distance = 900*1.4 = 1260m
# walk bus = 5 minutes -> 300 seconds
# bus distance = 300*1.4 = 1260m
route_me = text(f"""
    INSERT INTO network.transit_poi_paths (id, stop_id, gtfs, node_id, travel_time)
    SELECT
        :id AS id,
        :stop_id AS stop_id,
        :gtfs AS gtfs,
        node.id AS node_id,
        shortest_path.agg_cost AS travel_time
    FROM
        pgr_drivingdistance (
            'SELECT sw.id, sw.source, sw.target, sw.cost FROM network.sw_network sw',
            :source_node,
            CASE
                WHEN :mode LIKE '%bus' THEN 420
                ELSE 1260
            END,
            FALSE
        ) AS shortest_path
    JOIN network.sw_network_vertices_pgr node ON shortest_path.node = node.id;
    COMMIT;
""")


def get_transit_poi(dbname):
    """
    grabs the pois from the db
    """
    engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    query = "SELECT id, stop_id, gtfs, source_node, mode FROM network.transit_poi;"
    with engine.connect() as connection:
        return pd.read_sql(query, connection).to_dict(orient='records')


def process_transit_poi(poi, dbname):
    """
    each poi gets routed
    """
    engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{dbname}",
    poolclass=NullPool)

    with engine.connect() as connection:
        connection.execute(route_me, {
            'id': poi['id'],
            'stop_id': poi['stop_id'],
            'gtfs': poi['gtfs'],
            'source_node': poi['source_node'],
            'mode': poi['mode']
        })
    connection.close()
    engine.dispose()
    print(f"Processed POI {poi['id']}")


def polys(dbname):
    """
    create the polygons for the walkshed paths
    """
    engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    iso = text("""
        CREATE TABLE
            network.transit_poi_isochrones AS
        SELECT
            tp.id,
            tp.stop_id,
            tp.gtfs,
            ST_ConvexHull(ST_Collect(node.the_geom)) AS geom
        FROM
            network.transit_poi_paths tp
        JOIN network.sw_network_vertices_pgr node ON tp.node_id = node.id
        GROUP BY
            tp.id, tp.stop_id, tp.gtfs;
        COMMIT;   
        CREATE INDEX isochrones_geom_idx
        ON network.transit_poi_isochrones
        USING GIST (geom);
        COMMIT;
        """)
    with engine.connect() as connection:
        connection.execute(iso)

    connection.close()
    engine.dispose()