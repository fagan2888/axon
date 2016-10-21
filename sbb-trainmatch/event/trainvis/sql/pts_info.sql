SELECT
    p.*,
    h.is_outlier AS gh_outlier
FROM (
    SELECT *
    FROM (
        SELECT *
        FROM <POSTGRES_SCHEMA_PROVIDER>.sm_trip_link
        WHERE mot_segment_id = ANY( %(mot_id)s )
    ) t
    JOIN <POSTGRES_SCHEMA_PROVIDER>.sm_point_meta USING (segment_id)
    JOIN <POSTGRES_SCHEMA_PROVIDER>.sm_points USING (point_id)
    ) p
JOIN <POSTGRES_SCHEMA>.hotspots_gh11 h ON ST_GeoHash(ST_SetSRID(ST_MakePoint(p.lon,p.lat),4326), 11)=h.geohash11
;