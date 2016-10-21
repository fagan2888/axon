-- Need a temporary table to self-join
WITH a AS (
SELECT 
    m.mot_segment_id,
    m.is_start,
    l.datetime_created AT TIME ZONE l.tz AS datetime_created,
    l.tz,
    ST_Transform(stn.stop_coord,4326) AS coordinate,
    ST_Distance(COALESCE(x.coordinate,l.coordinate)::GEOGRAPHY , ST_Transform(stn.stop_coord,4326)::GEOGRAPHY ) AS distance,
    stn.stop_id_parent
FROM (
    SELECT 
        m1.id AS mot_segment_id,
        m1.start_location_id AS location_id,
        TRUE AS is_start
    FROM <POSTGRES_SCHEMA>.mot_segments m1
    WHERE m1.id = ANY( %(mot_id)s::uuid[] )
        
    UNION
    
    SELECT 
        m2.id AS mot_segment_id,
        m2.end_location_id AS location_id,
        FALSE AS is_start       
    FROM <POSTGRES_SCHEMA>.mot_segments m2
    WHERE m2.id = ANY( %(mot_id)s::uuid[] )
    ) m
-- Location point ID associated with start/end of segment
LEFT JOIN (
    SELECT 
        l1.id AS id,
        l1.mot_segment_id AS mot_segment_id,
        l1.vid AS vid,
        l1.datetime_created AS datetime_created,
        coalesce(l1.timezone_coordinate, l1.timezone_created) AS tz,
        l1.coordinate AS coordinate,
        COALESCE(h.is_outlier, FALSE) AS is_outlier
    FROM <POSTGRES_SCHEMA>.locations l1 
    LEFT JOIN <POSTGRES_SCHEMA>.hotspots_gh11 h ON ST_GeoHash(l1.coordinate, 11)=h.geohash11
    ) l ON m.mot_segment_id=l.mot_segment_id AND m.location_id=l.id
--  If location is an outlier replace with nearest (in time) non-outlier
LEFT JOIN LATERAL (
    SELECT 
        l2.id AS id,
        l2.mot_segment_id AS mot_segment_id,
        l2.vid AS vid,
        l2.datetime_created AS datetime_created,
        coalesce(l2.timezone_coordinate, l2.timezone_created) AS tz,
        l2.coordinate AS coordinate
    FROM <POSTGRES_SCHEMA>.locations l2
    LEFT JOIN <POSTGRES_SCHEMA>.hotspots_gh11 h2 ON ST_GeoHash(l2.coordinate, 11)=h2.geohash11
    WHERE l.vid=l2.vid
        AND l.id != l2.id
        AND h2.is_outlier IS FALSE
        AND ABS(EXTRACT (EPOCH FROM (l.datetime_created-l2.datetime_created))) < 600
    ORDER BY ABS(EXTRACT (EPOCH FROM (l.datetime_created-l2.datetime_created)))
    LIMIT 1
    ) x ON l.is_outlier IS TRUE
-- Find nearby stations
LEFT JOIN (
    SELECT DISTINCT
        stop_id_parent,
        ST_Transform(s.stop,3857) AS stop_coord
    FROM <POSTGRES_SCHEMA_PROVIDER>.stops s
    WHERE stop_id_parent IN (SELECT station_i FROM <POSTGRES_SCHEMA_PROVIDER>.stn_hops)
    ) stn ON ST_DWithin(ST_Transform(COALESCE(x.coordinate,l.coordinate),3857), stn.stop_coord, 10000)
)

SELECT DISTINCT ON(s.mot_segment_id)
    s.mot_segment_id,
    -- Start of journey specific data
    ST_Y(s.coordinate) AS lat_start,
    ST_X(s.coordinate) AS lon_start,
    s.datetime_created AS time_start,
    s.distance AS distance_start,
    s.stop_id_parent AS stop_id_start,
    s.tz AS timezone_start,
    -- End of journey specific d ata
    ST_Y(e.coordinate) AS lat_end,
    ST_X(e.coordinate) AS lon_end,
    e.datetime_created AS time_end,
    e.distance AS distance_end,
    e.stop_id_parent AS stop_id_end,
    e.tz AS timezone_end
FROM a s
JOIN a e USING (mot_segment_id) -- Self join, o
-- measure of connectivity between pairs of stations
INNER JOIN <POSTGRES_SCHEMA_PROVIDER>.stn_hops sh
    ON s.stop_id_parent=sh.station_i AND e.stop_id_parent=sh.station_j
-- Values needed for sorting but not returned 
JOIN LATERAL (
    SELECT
        CASE ((s.distance < (%(dist_lim)s + min(s.distance) OVER (PARTITION BY s.mot_segment_id)))
          AND (e.distance < (%(dist_lim)s + min(e.distance) OVER (PARTITION BY s.mot_segment_id))))
            WHEN TRUE THEN sh.n_hops
            ELSE NULL
        END AS is_near,
        (s.distance + e.distance) AS dist_tot
    ) v ON TRUE
WHERE s.is_start IS TRUE -- s contains all start station info
    AND e.is_start IS FALSE -- e contains all end station info
    AND n_hops > 0 -- disconnected stations labeled as -1
ORDER BY s.mot_segment_id NULLS LAST, v.is_near ASC NULLS LAST, v.dist_tot ASC NULLS LAST
;
