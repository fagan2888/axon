--> train_trips_leg
DELETE FROM <POSTGRES_SCHEMA>.train_trips_leg
WHERE journey_id IN (
  SELECT mot_segment_id
  FROM journeys_log jl
  JOIN (
    SELECT bound_from_id, MAX(end_visit_datetime) AS last_update
    FROM journeys_log
    GROUP BY bound_from_id
  ) a ON a.bound_from_id=jl.bound_from_id AND end_visit_datetime < last_update
  WHERE jl.bound_from_id=ANY(%(bfi)s)
);
--> Delete train_trips  (this must be done in SM because of the lag...)
DELETE FROM <POSTGRES_SCHEMA>.train_trips
WHERE id IN (
  SELECT mot_segment_id
  FROM journeys_log jl
  JOIN (
    SELECT bound_from_id, MAX(end_visit_datetime) AS last_update
    FROM journeys_log
    GROUP BY bound_from_id
  ) a ON a.bound_from_id=jl.bound_from_id AND end_visit_datetime < last_update
  WHERE jl.bound_from_id=ANY(%(bfi)s)
);
