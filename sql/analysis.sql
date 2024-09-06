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
CREATE OR REPLACE VIEW
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
    COALESCE(es.food_cnt, 0) AS food_cnt,
    COALESCE(es.hc_cnt, 0) AS hc_cnt,
    COALESCE(es.school_cnt, 0) AS school_cnt,
    COALESCE(open_space.os_check, 0) AS os_check,
    COALESCE(trails.trail_cnt, 0) AS trail_cnt,
    COALESCE(es.food_cnt, 0) + COALESCE(es.hc_cnt, 0) + COALESCE(open_space.os_check, 0) + COALESCE(es.school_cnt, 0) + COALESCE(trails.trail_cnt, 0) AS es_sum,
    COALESCE(j.sum_jobs, 0) AS sum_jobs
FROM
    input.census_blockgroups cb
    LEFT JOIN es_pt es ON cb.geoid = es.geoid
    LEFT JOIN open_space ON cb.geoid = open_space.geoid
    LEFT JOIN trails ON cb.geoid = trails.geoid
    LEFT JOIN output.lodes_jobs j ON cb.geoid = j.geoid;
COMMIT;

-- rank essential services 
CREATE OR REPLACE VIEW
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
CREATE OR REPLACE VIEW
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