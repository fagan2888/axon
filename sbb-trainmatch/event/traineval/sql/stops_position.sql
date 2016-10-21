SELECT
    stop_id,
    ST_X(stop) AS lon,
    ST_Y(stop) AS lat
FROM <POSTGRES_SCHEMA_PROVIDER>.stops s
WHERE s.stop_id = ANY( %(stop_id)s );