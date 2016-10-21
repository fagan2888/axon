CREATE TABLE train_trips (
    id integer NOT NULL,
    vid character varying(100) NOT NULL,
    connection_departure timestamp with time zone NOT NULL, -- does this refer to the first connection in the trip? Do we need this?
    connection_arrival timestamp with time zone NOT NULL, -- does this refer to the first connection in the trip? Do we need this?
    from_station character varying(200),
    from_station_id character varying(100),
    to_station character varying(200),
    to_station_id character varying(100),
    from_lat double precision NOT NULL, -- do we need this? What does this currently refer to? (station vs start of journey)
    to_lat double precision NOT NULL, -- do we need this? What does this currently refer to? (station vs start of journey)
    start_time timestamp with time zone NOT NULL,
    end_time timestamp with time zone NOT NULL,
    timezone_start character varying(200),
    timezone_end character varying(200),
    confidence double precision NOT NULL,
    batch_id integer,
    extraction_date timestamp with time zone DEFAULT now() NOT NULL,
    from_lon double precision NOT NULL,
    to_lon double precision NOT NULL,
    model_info json,
    reconstruction_id text
);

CREATE SEQUENCE train_trips_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE train_trips_id_seq OWNED BY train_trips.id;


CREATE TABLE train_trips_leg (
    id integer NOT NULL,
    journey_id integer NOT NULL,
    vid character varying(200) NOT NULL,
    from_station character varying(200) NOT NULL,
    to_station character varying(200) NOT NULL,
    planned_departure timestamp with time zone NOT NULL,
    actual_departure timestamp with time zone NOT NULL,
    planned_arrival timestamp with time zone NOT NULL,
    actual_arrival timestamp with time zone NOT NULL,
    timezone_departure character varying(200),
    timezone_arrival character varying(200),
    platform_arrival character varying(50) NOT NULL, -- This column didn't exist before. We don't send it to the user, should we store it anyways?
    platform_departure character varying(50) NOT NULL, -- Switched the order of this name for consistancy purposes
    transport_identifier character varying(100) NOT NULL,
    transport_code character varying(100),
    transport_line character varying(100),
    number_of_stops integer,
    model_info json,
    batch_id integer
);

CREATE SEQUENCE train_trips_leg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


CREATE TABLE train_trips_failed (
    mot_segment_id character varying(100) NOT NULL,
    failure_cause character varying(200) NOT NULL,
    datetime_created timestamp with time zone NOT NULL
);


ALTER TABLE ONLY train_trips ALTER COLUMN id SET DEFAULT nextval('train_trips_id_seq'::regclass);
ALTER TABLE ONLY train_trips_leg ALTER COLUMN id SET DEFAULT nextval('train_trips_leg_id_seq'::regclass);

ALTER TABLE ONLY train_trips_leg ADD CONSTRAINT train_trips_leg_pkey PRIMARY KEY (id);
ALTER TABLE ONLY train_trips ADD CONSTRAINT train_trips_pkey PRIMARY KEY (id);

CREATE INDEX train_trips_id_idx ON train_trips USING btree (id);
CREATE INDEX train_trips_leg_id_idx ON train_trips_leg USING btree (id);
CREATE INDEX train_trips_leg_vid_idx ON train_trips_leg USING btree (vid);
CREATE INDEX train_trips_vid_idx ON train_trips USING btree (vid);
CREATE INDEX train_trips_failed_idx ON train_trips_failed USING btree (mot_segment_id);