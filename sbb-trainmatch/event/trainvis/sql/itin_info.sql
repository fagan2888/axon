SELECT *
FROM <POSTGRES_SCHEMA_PROVIDER>.sm_itineraries i
JOIN <POSTGRES_SCHEMA_PROVIDER>.sm_diagnostics d USING (itinerary_id)
WHERE mot_segment_id = ANY( %(mot_id)s );