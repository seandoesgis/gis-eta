#!/bin/bash
# optimized for MacOS, requires tippecanoe

# db connection info from .env
required_vars=("USER" "PASSWORD" "HOST" "PORT")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: $var is not set in the .env file"
        exit 1
    fi
done

DB_NAME="eta"

OUTPUT_DIR="$HOME/gis-eta/output"

# function to query table to geojson while also transforming to 4326 
pg_to_geojson() {
    local query="$1"
    local output_file="$2"
    local geom_column="${3:-geometry}"
    
    local wrapped_query="
    SELECT jsonb_build_object(
        'type',     'FeatureCollection',
        'features', jsonb_agg(feature)
    )
    FROM (
      SELECT jsonb_build_object(
        'type',       'Feature',
        'geometry',   ST_AsGeoJSON(ST_Transform(${geom_column}, 4326))::jsonb,
        'properties', to_jsonb(row) - '*' - '${geom_column}'
      ) AS feature
      FROM ($query) row
    ) features"
    
    PGPASSWORD=$PASSWORD psql -h $HOST -p $PORT -d $DB_NAME -U $USER -c "COPY ($wrapped_query) TO STDOUT" > "$output_file"
}

mkdir -p "$OUTPUT_DIR"

pg_to_geojson "SELECT ROW_NUMBER() OVER () AS id, * FROM output.output" "$OUTPUT_DIR/output.geojson"
pg_to_geojson "SELECT ROW_NUMBER() OVER () AS id, stop_id, gtfs, geom as geometry FROM network.transit_poi_isochrones" "$OUTPUT_DIR/walksheds.geojson"
pg_to_geojson "WITH a AS (
 SELECT st.stop_id,
 t.gtfs,
 CASE WHEN r.route_type = 3 THEN json_agg(distinct(t.route_id)) ELSE '[]'::json END as routes,
 CASE WHEN r.route_type = 3 THEN '[]'::json ELSE json_agg(distinct(r.route_long_name)) END as route_names
 FROM septa_bus.stop_times st
 JOIN output.all_trips t ON st.trip_id::text = t.trip_id
 JOIN septa_bus.routes r ON t.route_id = r.route_id
 WHERE t.gtfs = 'septa_bus'::text
 GROUP BY st.stop_id, t.gtfs, r.route_type
 UNION ALL
 SELECT st.stop_id,
 t.gtfs,
 '[]'::json as routes,
 json_agg(distinct(r.route_long_name)) as route_names
 FROM septa_rail.stop_times st
 JOIN output.all_trips t ON st.trip_id = t.trip_id
 JOIN septa_rail.routes r ON t.route_id = r.route_id
 WHERE t.gtfs = 'septa_rail'::text
 GROUP BY st.stop_id, t.gtfs
 UNION ALL
 SELECT st.stop_id,
 t.gtfs,
 json_agg(distinct(t.route_id)) as routes,
 '[]'::json as route_names
 FROM njtransit_bus.stop_times st
 JOIN output.all_trips t ON st.trip_id::text = t.trip_id
 WHERE t.gtfs = 'njt_bus'::text
 GROUP BY st.stop_id, t.gtfs
 UNION ALL
 SELECT st.stop_id,
 t.gtfs,
 '[]'::json as routes,
 json_agg(distinct(r.route_long_name)) as route_names
 FROM njtransit_rail.stop_times st
 JOIN output.all_trips t ON st.trip_id::text = t.trip_id
 JOIN njtransit_rail.routes r ON t.route_id = r.route_id::text
 WHERE t.gtfs = 'njt_rail'::text
 GROUP BY st.stop_id, t.gtfs
 UNION ALL
 SELECT st.stop_id,
 t.gtfs,
 '[]'::json as routes,
 json_agg(distinct(r.route_long_name)) as route_names
 FROM patco.stop_times st
 JOIN output.all_trips t ON st.trip_id::text = t.trip_id
 JOIN patco.routes r ON t.route_id = r.route_id::text
 WHERE t.gtfs = 'patco'::text
 GROUP BY st.stop_id, t.gtfs
)
SELECT ROW_NUMBER() OVER () AS id, a.*, s.geom as geometry 
FROM a JOIN output.all_stops s ON a.stop_id = s.stop_id AND a.gtfs = s.gtfs JOIN input.census_blockgroups cb on st_intersects(s.geom,cb.geometry)" "$OUTPUT_DIR/transitstops.geojson"
pg_to_geojson "SELECT ROW_NUMBER() OVER () AS id, es.* FROM output.es_point_locations es, input.census_blockgroups cb where st_intersects(es.geometry,cb.geometry)" "$OUTPUT_DIR/es.geojson"
pg_to_geojson "SELECT ROW_NUMBER() OVER () AS id, os.* FROM input.open_space os" "$OUTPUT_DIR/os.geojson"

tippecanoe -o $OUTPUT_DIR/eta_score.mbtiles -l eta_score -f -r1 -pk -pf $OUTPUT_DIR/output.geojson
tippecanoe -o $OUTPUT_DIR/walksheds.mbtiles -l walksheds -f -r1 -pk -pf $OUTPUT_DIR/walksheds.geojson
tippecanoe -o $OUTPUT_DIR/transitstops.mbtiles -l transitstops -f -r1 -pk -pf $OUTPUT_DIR/transitstops.geojson
tippecanoe -o $OUTPUT_DIR/es.mbtiles -l es -f -r1 -pk -pf $OUTPUT_DIR/es.geojson
tippecanoe -o $OUTPUT_DIR/os.mbtiles -l os -f -r1 -pk -pf $OUTPUT_DIR/os.geojson

tile-join -n eta -pk -f -o $OUTPUT_DIR/eta.mbtiles $OUTPUT_DIR/eta_score.mbtiles $OUTPUT_DIR/walksheds.mbtiles $OUTPUT_DIR/transitstops.mbtiles $OUTPUT_DIR/es.mbtiles $OUTPUT_DIR/os.mbtiles

# rm $OUTPUT_DIR/*.geojson
rm $OUTPUT_DIR/eta_score.mbtiles $OUTPUT_DIR/walksheds.mbtiles $OUTPUT_DIR/transitstops.mbtiles $OUTPUT_DIR/es.mbtiles $OUTPUT_DIR/os.mbtiles