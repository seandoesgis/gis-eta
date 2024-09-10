-- apply acs data to blockgroup
CREATE OR REPLACE VIEW
    output.acs_bg AS
SELECT
    b11001_001e::INT AS hh,
    b01003_001e::INT AS pop,
    b22010_006e::INT + b22010_003e::INT AS hh1_dis,
    b17017_002e::INT AS hh_pov,
    b01001_020e::INT + b01001_021e::INT + b01001_022e::INT + b01001_023e::INT + b01001_024e::INT + b01001_025e::INT + b01001_044e::INT + b01001_045e::INT + b01001_046e::INT + b01001_047e::INT + b01001_048e::INT + b01001_049e::INT AS _65older,
    CONCAT(state, county, tract, "block group") AS geoid
FROM
    input.acs_data;
COMMIT;

-- rank vulnerable population inputs
CREATE TABLE
    output.vul_pop_rank AS
SELECT
    a.*,
    NTILE(10) OVER (ORDER BY hh_pov) AS hh_pov_quantile,
    NTILE(10) OVER (ORDER BY hh1_dis) AS hh1_dis_quantile,
    NTILE(10) OVER (ORDER BY _65older) AS _65older_quantile,
    (NTILE(10) OVER (ORDER BY hh_pov) + NTILE(10) OVER (ORDER BY hh1_dis) + NTILE(10) OVER (ORDER BY _65older)) / 3 AS vul_pop_rank -- average the 3 quantiles
FROM
    output.acs_bg a;
COMMIT;

-- summarize lodes job data by blockgroup
CREATE OR REPLACE VIEW
    output.lodes_jobs AS
SELECT
    LEFT(w_geocode::TEXT, 12) AS geoid,
    SUM(c000) AS sum_jobs
FROM
    input.lodes_data
GROUP BY
    LEFT(w_geocode::TEXT, 12);
COMMIT;

-- merge essential service point locations
CREATE OR REPLACE VIEW
    output.es_point_locations AS
SELECT
    'senior service' AS type,
    ss.geometry
FROM
    input.senior_srv ss
WHERE
    ss.confidence >= 0.6
UNION
SELECT
    'food store' AS type,
    gs.geometry
FROM
    input.grocery_store gs
WHERE
    gs.confidence >= 0.6
UNION
SELECT
    'health care' AS type,
    hc.geometry
FROM
    input.health_care hc
WHERE
    hc.confidence >= 0.6
UNION
SELECT
    'school' AS type,
    sps.geometry
FROM
    input.schools_post_secondary sps
UNION
SELECT
    'school' AS type,
    spr.geometry
FROM
    input.schools_private spr
UNION
SELECT
    'school' AS type,
    sp.geometry
FROM
    input.schools_public sp;
COMMIT;
    
-- spatial join essential service locations to blockgroup, add jobs data
CREATE OR REPLACE VIEW
    output.es_count AS
WITH
    es_pt AS (
        SELECT
            cb.geoid,
            SUM(CASE WHEN es.type = 'senior service' THEN 1 ELSE 0 END) AS ss_cnt,
            SUM(CASE WHEN es.type = 'food store' THEN 1 ELSE 0 END) AS food_cnt,
            SUM(CASE WHEN es.type = 'health care' THEN 1 ELSE 0 END) AS hc_cnt,
            SUM(CASE WHEN es.type = 'school' THEN 1 ELSE 0 END) AS school_cnt
        FROM
            input.census_blockgroups cb
            LEFT JOIN output.es_point_locations es ON ST_Intersects (cb.geometry, es.geometry)
        GROUP BY
            cb.geoid
    ),
    open_space AS (
        SELECT
            cb.geoid,
            CASE
                WHEN COUNT(os.geometry) > 0 THEN 1
                ELSE 0
            END AS os_check
        FROM
            input.census_blockgroups cb
            LEFT JOIN input.open_space os ON ST_Intersects (cb.geometry, os.geometry)
        GROUP BY
            cb.geoid
    ),
    trails AS (
        SELECT
            cb.geoid,
            COUNT(tr.geometry) AS trail_cnt
        FROM
            input.census_blockgroups cb
            LEFT JOIN (
                SELECT
                    NAME,
                    cb.geoid,
                    st_intersection (t.geometry, cb.geometry) AS geometry
                FROM
                    input.trails t
                    LEFT JOIN input.census_blockgroups cb ON ST_Intersects (t.geometry, cb.geometry)
            ) tr ON tr.geoid = cb.geoid
        GROUP BY
            cb.geoid
    )
SELECT
    cb.geoid,
    COALESCE(es.ss_cnt) as ss_cnt,
    COALESCE(es.food_cnt, 0) AS food_cnt,
    COALESCE(es.hc_cnt, 0) AS hc_cnt,
    COALESCE(es.school_cnt, 0) AS school_cnt,
    COALESCE(open_space.os_check, 0) AS os_check,
    COALESCE(trails.trail_cnt, 0) AS trail_cnt,
    COALESCE(es.ss_cnt, 0) + COALESCE(es.food_cnt, 0) + COALESCE(es.hc_cnt, 0) + COALESCE(open_space.os_check, 0) + COALESCE(es.school_cnt, 0) + COALESCE(trails.trail_cnt, 0) AS es_sum,
    COALESCE(j.sum_jobs, 0) AS sum_jobs
FROM
    input.census_blockgroups cb
    LEFT JOIN es_pt es ON cb.geoid = es.geoid
    LEFT JOIN open_space ON cb.geoid = open_space.geoid
    LEFT JOIN trails ON cb.geoid = trails.geoid
    LEFT JOIN output.lodes_jobs j ON cb.geoid = j.geoid;
COMMIT;

-- rank essential services 
CREATE TABLE
    output.es_rank AS
SELECT
    ec.*,
    NTILE(10) OVER (ORDER BY es_sum) AS es_quantile,
    NTILE(10) OVER (ORDER BY sum_jobs) AS jobs_quantile,
    (NTILE(10) OVER (ORDER BY es_sum) + NTILE(10) OVER (ORDER BY sum_jobs)) / 2 AS es_rank  -- average the 2 quantiles
FROM
    output.es_count ec;
COMMIT;

-- calculate the difference of vulnerable population rank and essential service rank for access gap
CREATE TABLE
    output.access_gap_rank AS
SELECT
    vpr.geoid,
    vpr.vul_pop_rank,
    esr.es_rank,
    vpr.vul_pop_rank - esr.es_rank AS access_gap_rank
FROM
    output.vul_pop_rank vpr
    JOIN output.es_rank esr ON vpr.geoid = esr.geoid;
COMMIT;

-- AM transit travel zones within 45 minutes count
CREATE OR REPLACE VIEW
    output.taz_transit_45min AS
WITH
    zone_count AS (
        SELECT DISTINCT (o_taz),
            COUNT(*) AS t_45min_zone_cnt
        FROM
            input.matrix_45min
        GROUP BY
            o_taz
    )
SELECT
    z.*,
    NTILE(10) OVER (ORDER BY t_45min_zone_cnt) AS t_zone_quantile,
    t.geometry
FROM
    zone_count z
    JOIN input.taz t ON z.o_taz = t.taz;
COMMIT;

-- essential Service count in AM transit travel zones within 45 minutes
CREATE OR REPLACE VIEW
    output.taz_45_es AS
WITH
    taz_45 AS (
        SELECT
            o_taz,
            d_taz,
            t.geometry
        FROM
            input.matrix_45min m
            JOIN input.taz t ON m.d_taz::INT = t.taz
    ),
    taz_45_es AS (
        SELECT
            o_taz,
            COUNT(esl.geometry) AS es_cnt
        FROM
            taz_45 t
            JOIN output.es_point_locations esl ON ST_Intersects(t.geometry, esl.geometry)
        GROUP BY
            o_taz
    )
SELECT
    *,
    NTILE(10) OVER (ORDER BY es_cnt) AS es_quantile
FROM
    taz_45_es;
COMMIT;

-- creating a function to normalize time from text field
CREATE
OR REPLACE FUNCTION normalize_time (VARCHAR) RETURNS TIME AS $$
DECLARE
    hour_part int;
    minute_part int;
    second_part int;
    total_seconds int;
    normalized_time time;
BEGIN
    hour_part := split_part($1, ':', 1)::int;
    minute_part := split_part($1, ':', 2)::int;
    second_part := split_part($1, ':', 3)::int;
    
    total_seconds := hour_part * 3600 + minute_part * 60 + second_part;
    
    normalized_time := (total_seconds % 86400) * interval '1 second';
    
    RETURN normalized_time;
END;
$$ LANGUAGE plpgsql;
COMMIT;

-- merging all transit stop locations from gtfs
CREATE MATERIALIZED VIEW
    output.all_stops AS
SELECT
    s.stop_id,
    'septa_bus' AS gtfs,
    CASE WHEN r.route_type = 3 THEN 'bus'
        ELSE 'rail'
        END AS mode,
    ST_Transform(ST_SetSRID(ST_Point(stop_lon, stop_lat), 4326), 26918)::geometry (POINT, 26918) AS geom
FROM
    septa_bus.stops s
JOIN septa_bus.stop_times st ON s.stop_id = st.stop_id
    JOIN septa_bus.trips t ON st.trip_id = t.trip_id
    JOIN septa_bus.routes r ON t.route_id = r.route_id
GROUP BY
    s.stop_id,
    r.route_type,
    s.stop_lon,
    s.stop_lat
UNION
SELECT
    s.stop_id,
    'septa_rail' AS gtfs,
    'rail' AS mode,
    ST_Transform(ST_SetSRID(ST_Point(stop_lon, stop_lat), 4326), 26918)::geometry (POINT, 26918) AS geom
FROM
    septa_rail.stops s
UNION
SELECT
    s.stop_id,
    'njt_rail' AS gtfs,
    'rail' AS mode,
    ST_Transform(ST_SetSRID(ST_Point(stop_lon, stop_lat), 4326), 26918)::geometry (POINT, 26918) AS geom
FROM
    njtransit_rail.stops s
UNION
SELECT
    s.stop_id,
    'njt_bus' AS gtfs,
    'bus' AS mode,
    ST_Transform(ST_SetSRID(ST_Point(stop_lon, stop_lat), 4326), 26918)::geometry (POINT, 26918) AS geom
FROM
    njtransit_bus.stops s
UNION
SELECT
    s.stop_id,
    'patco' AS gtfs,
    'rail' AS mode,
    ST_Transform(ST_SetSRID(ST_Point(stop_lon, stop_lat), 4326), 26918)::geometry (POINT, 26918) AS geom
FROM
    patco.stops s;
COMMIT;

-- finding wednesday service ids (picked 9/4/24) from gtfs
CREATE MATERIALIZED VIEW
    output.all_service AS
SELECT
    service_id::TEXT,
    'septa_bus' AS gtfs
FROM
    septa_bus.calendar
WHERE
    wednesday = 1
    AND 20240904 BETWEEN start_date AND end_date
UNION
SELECT
    service_id::TEXT,
    'septa_rail' AS gtfs
FROM
    septa_rail.calendar
WHERE
    wednesday = 1
    AND 20240904 BETWEEN start_date AND end_date
UNION
SELECT
    service_id::TEXT,
    'septa_bus' AS gtfs
FROM
    septa_bus.calendar_dates
WHERE
    date = '20240904'
    AND exception_type = 1
UNION
SELECT
    service_id,
    'septa_rail' AS gtfs
FROM
    septa_rail.calendar_dates
WHERE
    date = '20240904'
    AND exception_type = 1
UNION
SELECT
    service_id::TEXT,
    'njt_bus' AS gtfs
FROM
    njtransit_bus.calendar_dates
WHERE
    date = 20240904
    AND exception_type = 1
UNION
SELECT
    service_id::TEXT,
    'njt_rail' AS gtfs
FROM
    njtransit_rail.calendar_dates
WHERE
    date = 20240904
    AND exception_type = 1
UNION
SELECT
    service_id::TEXT,
    'patco' AS gtfs
FROM
    patco.calendar
WHERE
    wednesday = 1
    AND 20240904 BETWEEN start_date AND end_date;
COMMIT;

-- finding all trip_ids for wednesday 9/4/24 service from gtfs     
CREATE MATERIALIZED VIEW
    output.all_trips AS
SELECT
    t.trip_id::TEXT,
    t.route_id,
    t.service_id::TEXT,
    s.gtfs
FROM
    septa_bus.trips t
    JOIN output.all_service s ON t.service_id::TEXT = s.service_id
WHERE
    s.gtfs = 'septa_bus'
UNION
SELECT
    t.trip_id::TEXT,
    t.route_id::TEXT,
    t.service_id::TEXT,
    s.gtfs
FROM
    septa_rail.trips t
    JOIN output.all_service s ON t.service_id = s.service_id
WHERE
    s.gtfs = 'septa_rail'
UNION
SELECT
    t.trip_id::TEXT,
    t.route_id::TEXT,
    t.service_id::TEXT,
    s.gtfs
FROM
    njtransit_bus.trips t
    JOIN output.all_service s ON t.service_id::TEXT = s.service_id
WHERE
    s.gtfs = 'njt_bus'
UNION
SELECT
    t.trip_id::TEXT,
    t.route_id::TEXT,
    t.service_id::TEXT,
    s.gtfs
FROM
    njtransit_rail.trips t
    JOIN output.all_service s ON t.service_id::TEXT = s.service_id
WHERE
    s.gtfs = 'njt_rail'
UNION
SELECT
    t.trip_id::TEXT,
    t.route_id::TEXT,
    t.service_id::TEXT,
    s.gtfs
FROM
    patco.trips t
    JOIN output.all_service s ON t.service_id::TEXT = s.service_id
WHERE
    s.gtfs = 'patco';
COMMIT;

-- finding all stop times for service_id in the day time range from gtfs
CREATE MATERIALIZED VIEW
    output.all_stop_times AS
SELECT
    st.stop_id,
    t.gtfs,
    t.route_id,
    normalize_time (st.departure_time) AS departure_time
FROM
    septa_bus.stop_times st
    JOIN output.all_trips t ON st.trip_id::TEXT = t.trip_id
WHERE
    (normalize_time (st.departure_time) BETWEEN '00:00:00' AND '23:59:59')
    AND t.gtfs = 'septa_bus'
UNION
SELECT
    st.stop_id,
    t.gtfs,
    t.route_id,
    normalize_time (st.departure_time) AS departure_time
FROM
    septa_rail.stop_times st
    JOIN output.all_trips t ON st.trip_id::TEXT = t.trip_id
WHERE
    (normalize_time (st.departure_time) BETWEEN '00:00:00' AND '23:59:59')
    AND t.gtfs = 'septa_rail'
UNION
SELECT
    st.stop_id,
    t.gtfs,
    t.route_id,
    normalize_time (st.departure_time) AS departure_time
FROM
    njtransit_bus.stop_times st
    JOIN output.all_trips t ON st.trip_id::TEXT = t.trip_id
WHERE
    (normalize_time (st.departure_time) BETWEEN '00:00:00' AND '23:59:59')
    AND t.gtfs = 'njt_bus'
UNION
SELECT
    st.stop_id,
    t.gtfs,
    t.route_id,
    normalize_time (st.departure_time) AS departure_time
FROM
    njtransit_rail.stop_times st
    JOIN output.all_trips t ON st.trip_id::TEXT = t.trip_id
WHERE
    (normalize_time (st.departure_time) BETWEEN '00:00:00' AND '23:59:59')
    AND t.gtfs = 'njt_rail'
UNION
SELECT
    st.stop_id,
    t.gtfs,
    t.route_id,
    normalize_time (st.departure_time) AS departure_time
FROM
    patco.stop_times st
    JOIN output.all_trips t ON st.trip_id::TEXT = t.trip_id
WHERE
    (normalize_time (st.departure_time) BETWEEN '00:00:00' AND '23:59:59')
    AND t.gtfs = 'patco';
COMMIT;

-- creating stops table with daily departure stats
CREATE MATERIALIZED VIEW
    output.stops_w_departs AS
SELECT
    s.stop_id,
    s.gtfs,
    s.geom,
    COUNT(st.*) AS tot_departures
FROM
    output.all_stops s
    JOIN output.all_stop_times st ON s.stop_id = st.stop_id
    AND s.gtfs = st.gtfs
GROUP BY
    s.stop_id,
    s.gtfs,
    s.geom;
COMMIT;

CREATE INDEX stops_w_departs_idx
  ON output.stops_w_departs
  USING GIST (geom);
COMMIT;

-- calculate daily departs per taz
CREATE OR REPLACE VIEW
    output.taz_departs AS
WITH
    taz_departs AS (
        SELECT
            t.taz,
            COALESCE(SUM(s.tot_departures), 0) AS total_departures
        FROM
            INPUT.taz t
            LEFT JOIN output.stops_w_departs s ON ST_Intersects(s.geom, t.geometry)
        GROUP BY
            t.taz,
            t.geometry
    )
SELECT
    taz,
    NTILE(10) OVER (ORDER BY total_departures) AS depart_quantile
FROM
    taz_departs;
COMMIT;

-- creating route-able sidewalk network
CREATE TABLE
    network.sw_network AS
SELECT
    NULL::INTEGER AS source,
    NULL::INTEGER AS target,
    st_length (geom.geom) AS COST,
    geom.geom AS geometry
FROM
    (
        SELECT 
            (ST_Dump (geometry)).geom
        FROM
            input.pedestrian_network
    ) AS geom;
COMMIT;

ALTER TABLE network.sw_network
ADD COLUMN id serial PRIMARY KEY;
COMMIT;

CREATE INDEX sw_network_geom_idx ON NETWORK.sw_network USING GIST (geometry);
COMMIT;

-- topology
SELECT
    pgr_createTopology (
        'network.sw_network',
        0.001,
        'geometry',
        'id',
        clean := 'true'
    );

SELECT
    pgr_analyzeGraph ('network.sw_network', 0.001, 'geometry', 'id');

-- creating transit poi for walksheds
CREATE TABLE
    network.transit_poi AS
SELECT
    ROW_NUMBER() OVER (ORDER BY t.stop_id, t.gtfs ASC) AS id,
    t.stop_id,
    ST_ClosestPoint(t.geom, sw.the_geom) AS nearest_point,
    sw.id AS source_node,
    t.gtfs,
    t.mode
FROM
    output.all_stops AS t
    JOIN LATERAL (
        SELECT
            id,
            the_geom
        FROM
            network.sw_network_vertices_pgr AS sw
        WHERE
            ST_DWithin (t.geom, sw.the_geom, 300) -- filters points within 50 meters of sw network node
        ORDER BY
            ST_Distance (t.geom, sw.the_geom)
        LIMIT
            1
    ) AS sw ON TRUE;
COMMIT;

CREATE INDEX transit_poi_geom_idx
ON network.transit_poi
USING GIST (nearest_point);
COMMIT;

CREATE TABLE 
    network.transit_poi_paths (
        id INTEGER,
        stop_id VARCHAR(20),
        gtfs VARCHAR(20),
        node_id INTEGER,
        travel_time FLOAT
);