SELECT
    itinerary_id,
    leg_id,
    segment_id,
    point_id,
    row_number() OVER (
            PARTITION BY leg_id ORDER BY ST_LINELOCATEPOINT(geom, ST_SETSRID(ST_MAKEPOINT(lon, lat),4326)) ASC, time ASC, point_id ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as data_order,
    row_number() OVER (
			PARTITION BY leg_id ORDER BY time ASC, ST_LINELOCATEPOINT(geom, ST_SETSRID(ST_MAKEPOINT(lon, lat), 4326)) ASC, point_id ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as time_order
FROM temp_ooo
JOIN <POSTGRES_SCHEMA>.train_routes ON (
	train_routes.route_long_name = temp_ooo.route_long_name AND
	train_routes.agency_id = temp_ooo.agency_id AND
	train_routes.stop_id_start_parent = temp_ooo.stop_id_start AND
	train_routes.stop_id_end_parent = temp_ooo.stop_id_end
)
JOIN <POSTGRES_SCHEMA>.route_geoms ON (train_routes.geom_id=route_geoms.geom_id)
ORDER BY time ASC, data_order ASC;