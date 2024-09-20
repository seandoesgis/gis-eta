#!/bin/bash
# optimized for MacOS, requires tippecanoe

# db creds
DB_NAME="eta"
DB_USER=""
DB_PASSWORD=""
DB_HOST="localhost"
DB_PORT="5432"

OUTPUT_DIR="$HOME/gis-eta/output"

# function to query table to geojson while also transforming to 4326 
pg_to_geojson() {
    local query="$1"
    local output_file="$2"
    local geom_column="${3:-geometry}"  # Default to 'geom' if not specified
    
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
    
    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "COPY ($wrapped_query) TO STDOUT" > "$output_file"
}

mkdir -p "$OUTPUT_DIR"

pg_to_geojson "SELECT * FROM output.output" "$OUTPUT_DIR/output.geojson"
pg_to_geojson "SELECT id, stop_id, gtfs, geom as geometry FROM network.transit_poi_isochrones" "$OUTPUT_DIR/walksheds.geojson"
pg_to_geojson "SELECT s.stop_id, s.gtfs, s.geom as geometry FROM output.stops_w_departs s, input.census_blockgroups cb where st_intersects(s.geom,cb.geometry)" "$OUTPUT_DIR/transitstops.geojson"
pg_to_geojson "SELECT es.* FROM output.es_point_locations es, input.census_blockgroups cb where st_intersects(es.geometry,cb.geometry)" "$OUTPUT_DIR/es.geojson"

tippecanoe -o $OUTPUT_DIR/eta_score.mbtiles -l eta_score -f -r1 -pk -pf $OUTPUT_DIR/output.geojson
tippecanoe -o $OUTPUT_DIR/walksheds.mbtiles -l walksheds -f -r1 -pk -pf $OUTPUT_DIR/walksheds.geojson
tippecanoe -o $OUTPUT_DIR/transitstops.mbtiles -l transitstops -f -r1 -pk -pf $OUTPUT_DIR/transitstops.geojson
tippecanoe -o $OUTPUT_DIR/es.mbtiles -l es -f -r1 -pk -pf $OUTPUT_DIR/es.geojson

tile-join -n eta -pk -f -o $OUTPUT_DIR/eta.mbtiles $OUTPUT_DIR/eta_score.mbtiles $OUTPUT_DIR/walksheds.mbtiles $OUTPUT_DIR/transitstops.mbtiles $OUTPUT_DIR/es.mbtiles

rm /output/*.geojson
rm $OUTPUT_DIR/eta_score.mbtiles $OUTPUT_DIR/walksheds.mbtiles $OUTPUT_DIR/transitstops.mbtiles $OUTPUT_DIR/es.mbtiles