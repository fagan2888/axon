CREATE TABLE sbb.sm_diagnostics
(
  mot_segment_id text,
  itinerary_id text,
  count double precision,
  max bigint,
  min bigint,
  median bigint,
  mean bigint,
  n_time_in double precision,
  kde_weighed_avg bigint,
  warning_str text,
  warning_bool boolean
)
WITH (
  OIDS=FALSE
);
CREATE UNIQUE INDEX sm_diagnostics_itin_idx ON sbb.sm_diagnostics (itinerary_id);
CREATE INDEX sm_diagnostics_mot_idx ON sbb.sm_diagnostics (mot_segment_id);


CREATE TABLE sbb.sm_itineraries
(
  itinerary_id text,
  time_start timestamp without time zone,
  time_end timestamp without time zone,
  context_reconstruction text,
  num_legs double precision
)
WITH (
  OIDS=FALSE
);
CREATE UNIQUE INDEX sm_itineraries_idx ON sbb.sm_itineraries (itinerary_id);


CREATE TABLE sbb.sm_legs
(
  leg_id text,
  leg_number double precision,
  route_full_name text,
  route_category text,
  route_line text,
  route_number text,
  agency_id text,
  num_segments double precision,
  time_start timestamp without time zone,
  time_planned_start timestamp without time zone,
  stop_id_start text,
  platform_start text,
  station_name_start text,
  time_end timestamp without time zone,
  time_planned_end timestamp without time zone,
  stop_id_end text,
  platform_end text,
  station_name_end text,
  route_name text,
  nb_train_stops double precision,
  leg_type text
)
WITH (
  OIDS=FALSE
);
CREATE UNIQUE INDEX sm_legs_idx ON sbb.sm_legs (leg_id);
CREATE INDEX sm_legs_type_idx ON sbb.sm_legs (leg_type);


CREATE TABLE sbb.sm_points
(
  point_id bigint,
  lat double precision,
  lon double precision,
  "time" timestamp without time zone,
  horizontal_accuracy double precision,
  within_mot_segment boolean
)
WITH (
  OIDS=FALSE
);
CREATE INDEX sm_points_idx ON sbb.sm_points (point_id);


CREATE TABLE sbb.sm_points_fpga
(
  point_id bigint,
  segment_id text,
  lat double precision,
  lon double precision,
  "time" timestamp without time zone,
  horizontal_accuracy double precision,
  within_mot_segment boolean
)
WITH (
  OIDS=FALSE
);
CREATE INDEX sm_points_fpga_seg_idx ON sbb.sm_points_fpga (segment_id);
CREATE INDEX sm_points_fpga_idx ON sbb.sm_points_fpga (point_id);


CREATE TABLE sbb.sm_point_meta
(
  segment_id text,
  point_id bigint,
  distance double precision,
  ooo_outlier boolean,
  is_long_stop boolean
)
WITH (
  OIDS=FALSE
);
CREATE INDEX sm_point_meta_seg_idx ON sbb.sm_point_meta (segment_id);
CREATE INDEX sm_point_meta_pt_idx ON sbb.sm_point_meta (point_id);


CREATE TABLE sbb.sm_segments
(
  segment_id text,
  is_long_stop boolean,
  segment_number double precision,
  time_start timestamp without time zone,
  time_end timestamp without time zone,
  stop_id_start text,
  stop_id_end text,
  waypoint boolean
)
WITH (
  OIDS=FALSE
);
CREATE UNIQUE INDEX sm_segments_idx ON sbb.sm_segments (segment_id);


CREATE TABLE sbb.sm_stats
(
  mot_segment_id text,
  itinerary_id text,
  count double precision,
  n_time_in double precision,
  warning_bool boolean,
  warning_str text,
  min_value bigint,
  delta_next bigint,
  n2x bigint,
  confidence double precision
)
WITH (
  OIDS=FALSE
);
CREATE UNIQUE INDEX sm_stats_itin_idx ON sbb.sm_stats (itinerary_id);
CREATE INDEX sm_stats_mot_idx ON sbb.sm_stats (mot_segment_id);


CREATE TABLE sbb.sm_trip_link
(
  vid text,
  mot_segment_id text,
  itinerary_id text,
  leg_id text,
  segment_id text
)
WITH (
  OIDS=FALSE
);
CREATE INDEX sm_trip_link_vid_idx ON sbb.sm_trip_link (vid);
CREATE INDEX sm_trip_link_mot_idx ON sbb.sm_trip_link (mot_segment_id);
CREATE INDEX sm_trip_link_itin_idx ON sbb.sm_trip_link (itinerary_id);
CREATE INDEX sm_trip_link_leg_idx ON sbb.sm_trip_link (leg_id);
CREATE INDEX sm_trip_link_seg_idx ON sbb.sm_trip_link (segment_id);


CREATE TABLE sbb.sm_trips
(
  mot_segment_id text,
  lat_start double precision,
  lon_start double precision,
  time_start timestamp without time zone,
  distance_start double precision,
  stop_id_start text,
  timezone_start text,
  lat_end double precision,
  lon_end double precision,
  time_end timestamp without time zone,
  distance_end double precision,
  stop_id_end text,
  timezone_end text,
  vid text,
  trip_time_start timestamp without time zone,
  trip_time_end timestamp without time zone,
  visit_id_start bigint,
  visit_id_end bigint
)
WITH (
  OIDS=FALSE
);
CREATE UNIQUE INDEX sm_trips_mot_idx ON sbb.sm_trips (mot_segment_id);
CREATE INDEX sm_trips_cid_idx ON sbb.sm_trips (vid);
