WITH vidtime AS (
    SELECT DISTINCT ON(mot_segment_id)
        mot_segment_id,
        vid,
        datetime_created AS datetime_created --AT TIME ZONE coalesce(timezone_coordinate, timezone_created)
    FROM <POSTGRES_SCHEMA>.locations
    WHERE mot_segment_id IN %(mot_id)s
    ORDER BY mot_segment_id
)
SELECT
    a.vid,
    vidtime.mot_segment_id AS mot_segment_id,
    a.starttriptime AT TIME ZONE a.tz AS trip_time_start,
    a.endtriptime AT TIME ZONE a.tz  AS trip_time_end,
    a.visit_id_lag AS visit_id_start,
    a.visit_id AS visit_id_end
FROM (
    SELECT
        vid,
        LEAD(id,1) OVER(PARTITION BY vid ORDER BY datetime_arrived ASC) as visit_id,
        datetime_departed as starttriptime,
        id as visit_id_lag,
        COALESCE(LEAD(datetime_arrived,1) OVER(PARTITION BY vid ORDER BY datetime_arrived ASC), NOW()) as endtriptime,
        coalesce(timezone_coordinate, timezone_created) AS tz
    FROM <POSTGRES_SCHEMA>.visits
    WHERE vid in (SELECT DISTINCT vid FROM vidtime) AND (datetime_departed - datetime_arrived) > interval '%(min_visit_dur)s seconds'
) a
INNER JOIN vidtime
    ON vidtime.vid=a.vid
    AND vidtime.datetime_created BETWEEN a.starttriptime AND a.endtriptime
;