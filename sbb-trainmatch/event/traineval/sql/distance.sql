SELECT
    triproute.segment_id,
	locations.horizontal_accuracy,
	ST_DISTANCE(coordinate::geography, ST_LINEINTERPOLATEPOINT(geometry.geom, timefrac)::geography) as distance,
	locations.datetime_created AT TIME ZONE 'Europe/Zurich' as time,
	ST_Y(coordinate) as lat,
	ST_X(coordinate) as lon,
	locations.id as point_id
FROM (
-- For each segment, this gets the geometry associated with it. The distinct on exists because the same station parent
-- can have multiple "stations" associated with it, usually followed by a colon (e.g. the station 123456 may be in the
-- database as 123456:2, 123456:4, 123456:3 and 123456:7.
    SELECT
	    DISTINCT (vid || train_routes.geom_id::text || temp_segments.time_start::text || temp_segments.time_end::text),
	    temp_segments.segment_id,
	    temp_segments.vid,
	    temp_segments.route_name,
	    temp_segments.agency_id,
	    temp_segments.stop_id_start,
	    temp_segments.stop_id_end,
	    temp_segments.time_start,
	    temp_segments.time_end,
	    train_routes.geom_id
    FROM temp_segments
    LEFT JOIN <POSTGRES_SCHEMA>.train_routes ON (
        train_routes.route_long_name=temp_segments.route_name AND
        train_routes.agency_id=temp_segments.agency_id AND
        train_routes.stop_id_start_parent=temp_segments.stop_id_start AND
        train_routes.stop_id_end_parent=temp_segments.stop_id_end
    )
) triproute
LEFT JOIN <POSTGRES_SCHEMA>.route_geoms ON (
    triproute.geom_id=route_geoms.geom_id)
-- This finds the location points which occur within this segment and joins them on. Note that there is no criteria
-- on horizontal_accuracy or anything. We deal with those later.
JOIN <POSTGRES_SCHEMA>.locations ON (
    locations.vid=triproute.vid AND
    locations.datetime_created>(triproute.time_start::timestamp AT TIME ZONE 'Europe/Zurich') AND
    locations.datetime_created<=(triproute.time_end::timestamp AT TIME ZONE 'Europe/Zurich')
),
-- This lateral join takes calculates the fraction the location point is between the segment start and end times and
-- returns that as a float
LATERAL (
    SELECT
        COALESCE(
            EXTRACT(EPOCH FROM (locations.datetime_created - (triproute.time_start::timestamp AT TIME ZONE 'Europe/Zurich'))) /
            EXTRACT(EPOCH FROM (NULLIF(triproute.time_end, triproute.time_start)-triproute.time_start))
        ,1) as timefrac
    ) time_frac,
-- The next two lateral joins get complicated. If we weren't able to find a geom we replace it with a geometry that we
-- generate. This can happen if we have a walking segment between two train lines. For example, Bern train station
-- has Bern and Bern RBS, which are technically two different train stations. The connecting segment between Bern and
-- Bern RBS doesn't exist in the trip_routes table. The geometry we generate is just a line connecting the start and
-- end station of this segment. This has the advantage of failing gracefully if we really screwed up and don't have
-- a real train route in the train_routes table, we just approximate it with a straight line.
LATERAL (
    SELECT
        ST_MAKELINE(stop) as stationgeom
    FROM <POSTGRES_SCHEMA_PROVIDER>.stops
    WHERE
        stops.stop_id_parent=triproute.stop_id_start OR
        stops.stop_id_parent=triproute.stop_id_end
    ) station,
LATERAL ( SELECT COALESCE(route_geoms.geom, station.stationgeom) as geom) geometry;