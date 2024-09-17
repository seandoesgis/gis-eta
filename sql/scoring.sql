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

-- walkshed intersection (this takes some time)
CREATE MATERIALIZED VIEW
    output.transit_ws AS
WITH
    walksheds AS (
        SELECT
            ST_Union(ws.geom) AS geom
        FROM
            network.transit_poi_isochrones ws
    ),
    intersected_areas AS (
        SELECT
            cb.geoid,
            cb.geometry,
            ST_Area(ST_Intersection(ws.geom, cb.geometry)) / ST_AREA(cb.geometry) AS intersection_percent
        FROM
            walksheds ws
        JOIN input.census_blockgroups cb ON ST_Intersects(cb.geometry, ws.geom)
    )
SELECT
    intersected_areas.geoid,
    intersected_areas.geometry,
    intersected_areas.intersection_percent,
    NTILE(10) OVER (ORDER BY intersected_areas.intersection_percent DESC) AS walkshed_quantile
FROM
    intersected_areas;
COMMIT;

-- combine and calculate transit ranks
CREATE TABLE output.transit_rank AS
SELECT
    cb.geoid,
    tes.es_cnt as t_es_cnt,
    tj.jobs as t_jobs_cnt,
    tej.t_45_es_job_avg,
    tm.t_45min_zone_cnt,
    tm.t_zone_quantile,
    td.total_departures,
    td.depart_quantile,
    tw.walkshed_quantile,
    (tej.t_45_es_job_avg + tm.t_zone_quantile + td.depart_quantile + tw.walkshed_quantile) / 4 AS transit_access_rank
FROM
    input.census_blockgroups cb
LEFT JOIN 
    output.transit_45_es tes ON cb.geoid = tes.geoid
LEFT JOIN 
    output.transit_45_jobs tj ON cb.geoid = tj.geoid
LEFT JOIN 
    output.transit_45_es_job tej ON cb.geoid = tej.geoid
LEFT JOIN 
    output.transit_45min tm ON cb.geoid = tm.geoid
LEFT JOIN 
    output.transit_departs td ON cb.geoid = td.geoid
LEFT JOIN 
    output.transit_ws tw ON cb.geoid = tw.geoid;
COMMIT;

-- create ETA blockgroup output and calculate the total ETA score
CREATE TABLE output.output as
    SELECT 
        cb.geoid,
        bmc.mun1,
        bmc.mun2,
        vpr.hh, 
        vpr.pop, 
        vpr.hh1_dis, 
        vpr.hh_pov, 
        vpr."_65older",
        vpr.vul_pop_rank,
        er.ss_cnt,
        er.food_cnt,
        er.hc_cnt,
        er.school_cnt,
        er.os_check,
        er.trail_cnt,
        er.es_sum,
        er.sum_jobs,
        er.es_rank,
        agr.access_gap_rank,
        coalesce(tr.t_45min_zone_cnt,0) AS t_45min_zone_cnt,
        coalesce(tr.t_zone_quantile,10) AS t_zone_quantile,
        coalesce(tr.t_es_cnt,0) AS t_es_cnt,
        coalesce(tr.t_jobs_cnt,0) AS t_jobs_cnt,
        coalesce(tr.t_45_es_job_avg,10) AS t_45_es_job_avg, 
        coalesce(tr.total_departures,0) AS total_departures,
        coalesce(tr.depart_quantile,10) AS depart_quantile,
        coalesce(tr.walkshed_quantile,10) AS walkshed_quantile,
        coalesce(tr.transit_access_rank,10) AS transit_access_rank,
        agr.access_gap_rank * (coalesce(tr.transit_access_rank,10)) AS eta_score,
        cb.geometry
    FROM 
        input.census_blockgroups cb
    LEFT JOIN 
        output.bg_muni_crosswalk bmc on cb.geoid = bmc.geoid
    LEFT JOIN 
        output.vul_pop_rank vpr on cb.geoid = vpr.geoid
    LEFT JOIN 
        output.es_rank er on cb.geoid = er.geoid
    LEFT JOIN 
        output.access_gap_rank agr on cb.geoid = agr.geoid
    LEFT JOIN
        output.transit_rank tr on cb.geoid = tr.geoid;
COMMIT;