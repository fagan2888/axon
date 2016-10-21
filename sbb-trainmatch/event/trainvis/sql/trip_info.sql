SELECT *
FROM <POSTGRES_SCHEMA_PROVIDER>.sm_trips t
JOIN <POSTGRES_SCHEMA_PROVIDER>.sm_stats s USING (mot_segment_id)
-- This is a header, WHERE clauses are added in the code
