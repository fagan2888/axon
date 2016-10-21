SELECT
    p.*,
    FALSE AS gh_outlier
FROM (
    SELECT *
    FROM (
        SELECT *
        FROM <POSTGRES_SCHEMA_PROVIDER>.sm_trip_link
        WHERE mot_segment_id = ANY( %(mot_id)s )
    ) t
    JOIN <POSTGRES_SCHEMA_PROVIDER>.sm_point_meta USING (segment_id)
    JOIN <POSTGRES_SCHEMA_PROVIDER>.sm_points_fpga USING (segment_id, point_id)
    ) p
;