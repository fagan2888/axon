UPDATE train_trips_leg
SET
    actual_departure = CASE WHEN t1.actual_departure_time IS NOT NULL THEN t1.actual_departure_time ELSE train_trips_leg.actual_departure END,
    actual_arrival = CASE WHEN t1.actual_arrival_time IS NOT NULL THEN t1.actual_arrival_time ELSE train_trips_leg.actual_arrival END
FROM
(
    SELECT
        ttl.id AS id,
        ttl.planned_departure + (interval '1 minute' * live_departure.departure_delay_minutes) as actual_departure_time,
        ttl.planned_arrival + (interval '1 minute' * live_arrival.arrival_delay_minutes) as actual_arrival_time
    FROM train_trips_leg ttl
    LEFT JOIN live_train_stops live_departure
        ON ttl.transport_identifier = live_departure.train_name
        AND ttl.planned_departure = live_departure.planned_departure_datetime
    LEFT JOIN live_train_stops live_arrival
        ON ttl.transport_identifier = live_arrival.train_name
        AND ttl.planned_arrival = live_arrival.planned_arrival_datetime
    WHERE ttl.journey_id = ANY(%(mot_segment_ids)s::uuid[])
    AND NOT EXISTS ( -- filter out earlier extractions.
        SELECT 1 FROM live_train_stops
        WHERE train_name = live_departure.train_name
        AND planned_departure_datetime = live_departure.planned_departure_datetime
        AND extraction_timestamp > live_departure.extraction_timestamp
        LIMIT 1)
    AND NOT EXISTS ( -- filter out earlier extractions
        SELECT 1 FROM live_train_stops
        WHERE train_name = live_arrival.train_name
        AND planned_arrival_datetime = live_arrival.planned_arrival_datetime
        AND extraction_timestamp > live_arrival.extraction_timestamp
        LIMIT 1)
        ) t1
WHERE train_trips_leg.id = t1.id;