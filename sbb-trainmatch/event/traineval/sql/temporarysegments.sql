-- This is a stub for a bulk insert of segments

DROP TABLE IF EXISTS temp_segments;
CREATE TEMPORARY TABLE temp_segments (
    segment_id uuid,
    vid character varying(200),
    route_name character varying(200),
    agency_id character varying(200),
    time_start timestamp,
    time_end timestamp,
    stop_id_start character varying(200),
    stop_id_end character varying(200)
);
INSERT INTO temp_segments VALUES
