SELECT *
FROM <POSTGRES_SCHEMA_PROVIDER>.sm_legs l
JOIN (
	SELECT DISTINCT mot_segment_id, itinerary_id, leg_id
	FROM <POSTGRES_SCHEMA_PROVIDER>.sm_trip_link
	WHERE mot_segment_id = ANY( %(mot_id)s )
) t USING (leg_id)
WHERE leg_type='';