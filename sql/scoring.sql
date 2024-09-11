-- rank vulnerable population inputs, average the quantiles
CREATE TABLE
    output.vul_pop_rank AS
SELECT
    a.*,
    NTILE(10) OVER (ORDER BY hh_pov) AS hh_pov_quantile,
    NTILE(10) OVER (ORDER BY hh1_dis) AS hh1_dis_quantile,
    NTILE(10) OVER (ORDER BY _65older) AS _65older_quantile,
    (NTILE(10) OVER (ORDER BY hh_pov) + NTILE(10) OVER (ORDER BY hh1_dis) + NTILE(10) OVER (ORDER BY _65older)) / 3 AS vul_pop_rank
FROM
    output.acs_bg a;
COMMIT;

-- rank essential services and average the quantiles
CREATE TABLE
    output.es_rank AS
SELECT
    ec.*,
    NTILE(10) OVER (ORDER BY es_sum) AS es_quantile,
    NTILE(10) OVER (ORDER BY sum_jobs) AS jobs_quantile,
    (NTILE(10) OVER (ORDER BY es_sum) + NTILE(10) OVER (ORDER BY sum_jobs)) / 2 AS es_rank
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

-- translate taz quantiles to blockgroup using weighted average (area intersecting)
CREATE OR REPLACE VIEW
    output.bg_transit_45min AS
WITH
    intersected_areas AS (
        SELECT
            t.o_taz,
            cb.geoid,
            ST_Area(cb.geometry) AS cb_total_area,
            ST_Area(ST_Intersection(t.geometry, cb.geometry)) AS intersection_area,
            t.t_zone_quantile
        FROM
            INPUT.census_blockgroups cb,
            output.taz_transit_45min t
        WHERE
            ST_Intersects(cb.geometry, t.geometry)
    )
SELECT
    geoid,
    ROUND(SUM((intersection_area / cb_total_area) * t_zone_quantile)
    ) AS t45_quantile
FROM
    intersected_areas
GROUP BY
    geoid;
COMMIT;

CREATE OR REPLACE VIEW
    output.bg_45min_es AS
WITH
    intersected_areas AS (
        SELECT
            t.o_taz,
            cb.geoid,
            ST_Area(cb.geometry) AS cb_total_area,
            ST_Area(ST_Intersection(t.geometry, cb.geometry)) AS intersection_area,
            t.es_quantile
        FROM
            INPUT.census_blockgroups cb
            LEFT JOIN output.taz_45_es t ON ST_Intersects(cb.geometry, t.geometry)
    )
SELECT
    i.geoid,
    ROUND(SUM((intersection_area / cb_total_area) * es_quantile)) AS t_es_quantile
FROM
    intersected_areas i
GROUP BY
    i.geoid;
COMMIT;

CREATE OR REPLACE VIEW
    output.bg_departs AS
WITH
    intersected_areas AS (
        SELECT
            t.taz,
            cb.geoid,
            ST_Area(cb.geometry) AS cb_total_area,
            ST_Area(ST_Intersection(t.geometry, cb.geometry)) AS intersection_area,
            t.depart_quantile
        FROM
            INPUT.census_blockgroups cb,
            output.taz_departs t
        WHERE
            ST_Intersects(cb.geometry, t.geometry)
    )
SELECT
    geoid,
    ROUND(SUM((intersection_area / cb_total_area) * depart_quantile)) AS t_depart_quantile
FROM
    intersected_areas
GROUP BY
    geoid;
COMMIT;