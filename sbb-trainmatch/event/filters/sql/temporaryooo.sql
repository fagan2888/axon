DROP TABLE IF EXISTS temp_ooo;
CREATE TEMPORARY TABLE temp_ooo (
    itinerary_id UUID,
    leg_id UUID,
    segment_id UUID,
    point_id integer,
    route_long_name character varying(200),
    agency_id character varying(200),
    stop_id_start character varying(200),
    stop_id_end character varying(200),
    time timestamp with time zone,
    lat double precision,
    lon double precision
);
INSERT INTO temp_ooo VALUES
