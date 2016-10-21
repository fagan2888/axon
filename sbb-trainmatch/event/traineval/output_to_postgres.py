import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
import logging


try:
    from ..dbutil.temptable_template import temptable_template
except:
    from dbutil.temptable_template import temptable_template


def save_output(trip_link, trips, itineraries, segments, legs, points, point_meta, stats, diagnostics, loc_bounds, DB):

    # Upload the visualization data (sm_* tables)
    # truncate_all_sm_tables(DB)
    save_sm_output(trip_link, trips, itineraries, segments, legs, points, point_meta, stats, diagnostics, DB)

    # Reformat data
    trip_link_subset = subset_best_itineraries(trip_link.reset_index(), stats)
    train_trips, train_trips_leg = gen_output_tables(trips, trip_link_subset, itineraries, legs, stats, loc_bounds)
    # Upload to postgres
    # Output: train_trips and train_trips_leg tables in <schema> public on Dev
    update_postgres(train_trips, 'train_trips', DB)
    update_postgres(train_trips_leg, 'train_trips_leg', DB)
    clear_reprocessed_trips(train_trips, DB)
    # Hack to update train_trips_leg table with train delay info
    update_train_trips_leg_with_delays(trips, DB)

    return


def subset_best_itineraries(trip_link, stats):
    """ Prunes the trip_link table to contain only selected itineraries.
    """
    return trip_link.loc[np.in1d(trip_link.itinerary_id, stats.itinerary_id.values), :]


def gen_output_tables(trips, trip_link, itineraries, legs, stats, loc_bounds):

    # TODO : model info not defined right now, feed in any JSON as needed

    train_trips = build_train_trips_df(trip_link, trips, itineraries, legs, stats, loc_bounds)
    train_trips_leg = build_train_trips_leg_df(trip_link, trips, legs)

    return train_trips, train_trips_leg


def build_train_trips_df(trip_link, trips, itineraries, legs, stats, loc_bounds):

    # Time for the big join!
    merged = pd.merge(trip_link[['vid', 'mot_segment_id', 'itinerary_id']].drop_duplicates(),
                      trips,
                      on=['vid', 'mot_segment_id'], how='inner')
    merged = pd.merge(merged,
                      itineraries.reset_index(),
                      on='itinerary_id', how='inner', suffixes=('', '_itin'))
    merged = pd.merge(merged,
                      stats[['itinerary_id', 'confidence']],
                      on='itinerary_id', how='inner', suffixes=('', '_stats'))

    # Station names are returned within the legs while we only have the IDs in the trip
    merged = pd.merge(merged, build_stn_names_df(trip_link, legs, start=True),
                      on='itinerary_id', how='inner', suffixes=('', '_start'))
    merged = pd.merge(merged, build_stn_names_df(trip_link, legs, start=False),
                      on='itinerary_id', how='inner', suffixes=('', '_start'))

    merged = pd.merge(merged, loc_bounds, on='mot_segment_id', how='left', suffixes=('', '_bounds'))

    extraction_time = datetime.utcnow()

    col_names = ['id',                  # trips.mot_segment_id
                 'vid',                 # trips.vid
                 'connection_departure',  # itineraries.time_start
                 'connection_arrival',  # itineraries.time_end
                 'from_station',        # itineraries.  ???
                 'from_station_id',     # itineraries.  ???
                 'to_station',          # itineraries.  ???
                 'to_station_id',       # itineraries.  ???
                 'from_lat',            # trips.lat_start
                 'to_lat',              # trips.lat_end
                 'start_time',          # trips.trip_time_start
                 'end_time',            # trips.trip_time_end
                 'timezone_start',      # trips.???
                 'timezone_end',        # trips.???
                 'confidence',          # stat. ???
                 # 'batch_id',            # trips. ???
                 'extraction_date',     # ???
                 'from_lon',            # trips.lon_start
                 'to_lon',              # trips.lon_end
                 'model_info',          # ???
                 'reconstruction_id',   # itineraries.context_reconstruction
                 'bound_from_id',
                 'bound_to_id'
                 ]

    values = {'id': merged.mot_segment_id,
              'vid': merged.vid,
              'connection_departure': merged.apply(lambda x: timezone(x['timezone_start']).localize(x['time_start_itin']), axis=1),
              'connection_arrival': merged.apply(lambda x: timezone(x['timezone_end']).localize(x['time_end_itin']), axis=1),
              'from_station': merged.station_name_start,
              'from_station_id': merged.stop_id_start,
              'to_station': merged.station_name_end,
              'to_station_id': merged.stop_id_end,
              'from_lat': merged.lat_start,
              'to_lat': merged.lat_end,
              'start_time': merged.apply(lambda x: timezone(x['timezone_start']).localize(x['trip_time_start']), axis=1),
              'end_time': merged.apply(lambda x: timezone(x['timezone_end']).localize(x['trip_time_end']), axis=1),
              'timezone_start': merged.timezone_start,
              'timezone_end': merged.timezone_end,
              'confidence': merged.confidence,
              # 'batch_id': batch_id,
              'extraction_date': extraction_time,
              'from_lon': merged.lon_start,
              'to_lon': merged.lon_end,
              'model_info': '{}',
              'reconstruction_id': merged.context_reconstruction,
              'bound_from_id': merged.bound_from_id,
              'bound_to_id': merged.bound_to_id
              }

    df = pd.DataFrame(values, columns=col_names)

    return df


def build_stn_names_df(trip_link, legs, start=True):

    if start:
        col_names = ['time_start', 'stop_id_start', 'station_name_start']
        sort_asc = True
    else:
        col_names = ['time_end', 'stop_id_end', 'station_name_end']
        sort_asc = False

    stn_names = pd.merge(trip_link[['itinerary_id', 'leg_id']],
                         legs.loc[legs['leg_type'] == '', col_names].reset_index(),
                         on='leg_id')

    out = stn_names.sort_values(['itinerary_id', col_names[0]], ascending=sort_asc).groupby('itinerary_id').first()

    return out[col_names[1:]].reset_index()


def build_train_trips_leg_df(trip_link, trips, legs):

    # Time for the big join!
    merged = pd.merge(trip_link[['vid', 'mot_segment_id', 'leg_id']].drop_duplicates(),
                      trips,
                      on=['vid', 'mot_segment_id'], how='inner')
    merged = pd.merge(merged,
                      legs.reset_index(),
                      on='leg_id', how='inner', suffixes=('', '_itin'))

    # TODO : timezones in the legs don't make sense here -- they should be evaluated from a lat/lon

    col_names = ['id',                  # legs.leg_id
                 'journey_id',          # trips.mot_segment_id
                 'vid',                 # trips.vid
                 'from_station',        # legs.station_name_start
                 'to_station',          # legs.station_name_end
                 'planned_departure',   # legs.
                 'actual_departure',    # legs.time_start
                 'planned_arrival',     # legs.
                 'actual_arrival',      # legs.time_end
                 'timezone_departure',  # if start/end timezone identical, else NULL? (geo lookup postgres later)
                 'timezone_arrival',    # ^^^
                 'platform_arrival',    # legs.
                 'platform_departure',  # legs.
                 'transport_identifier',  # legs.route_full_name
                 'transport_code',      # legs.route_number
                 'transport_line',      # legs.route_line
                 'number_of_stops',
                 'model_info',
                 # 'batch_id',
                 'transport_provider',  # legs.route_category
                 'from_station_id',     # legs.stop_id_start
                 'to_station_id'        # legs.stop_id_end
                 ]

    values = {'id': merged.leg_id,
              'journey_id': merged.mot_segment_id,
              'vid': merged.vid,
              'from_station': merged.station_name_start,
              'to_station': merged.station_name_end,
              'planned_departure': merged.apply(lambda x: timezone(x['timezone_start']).localize(x['time_planned_start']), axis=1),
              'actual_departure':merged.apply(lambda x: timezone(x['timezone_start']).localize(x['time_start_itin']), axis=1),
              'planned_arrival': merged.apply(lambda x: timezone(x['timezone_end']).localize(x['time_planned_end']), axis=1),
              'actual_arrival': merged.apply(lambda x: timezone(x['timezone_end']).localize(x['time_end_itin']), axis=1),
              'timezone_departure': merged.timezone_start,
              'timezone_arrival': merged.timezone_start,
              'platform_departure': merged.platform_start.fillna('unknown'),
              'platform_arrival': merged.platform_end.fillna('unknown'),
              'transport_identifier': merged.route_full_name.fillna(''),
              'transport_code': merged.route_number.fillna(''),
              'transport_line': merged.route_line.fillna(''),
              'number_of_stops': merged.nb_train_stops,
              'model_info': '{}',
              # 'batch_id': batch_id,
              'transport_provider': merged.route_category.fillna(merged.leg_type),
              'from_station_id': merged.stop_id_start_itin,
              'to_station_id': merged.stop_id_end_itin
              }

    df = pd.DataFrame(values, columns=col_names)

    return df


def update_postgres(source, table_name, DB):

    table_properties = {
        'table_name': table_name,
        'column_names': ', '.join(source.columns.values)
    }

    header_str = 'INSERT INTO <POSTGRES_SCHEMA>.{table_name} ({column_names}) VALUES \n'
    header_str = DB.replace_schemas(header_str.format(**table_properties))
    single_row_str = '(%('+')s, %('.join(source.columns.values)+')s)'

    row_list = []
    for index, row in source.iterrows():
        row_sql = DB.query_mogrify(single_row_str, row.to_dict())
        row_list.append(row_sql)

    full_sql_str = header_str + ',\n'.join(row_list) + ';'

    DB.execute(full_sql_str)
    return


def save_sm_output(trip_link, trips, itineraries, segments, legs, points, point_meta, stats, diagnostics, DB):
    """
    Saves all the dataframe so they can be loaded by the visualization package
    """



    single_df_insert(trips, 'sm_trips', DB)
    single_df_insert(trip_link.reset_index(), 'sm_trip_link', DB)
    single_df_insert(itineraries.reset_index(), 'sm_itineraries', DB)
    single_df_insert(legs.reset_index(), 'sm_legs', DB)
    single_df_insert(segments.reset_index(), 'sm_segments', DB)

    points, points_fpga = split_fpga(points.reset_index(), point_meta.reset_index())
    single_df_insert(points, 'sm_points', DB)
    single_df_insert(point_meta.reset_index(), 'sm_point_meta', DB)
    single_df_insert(points_fpga.reset_index(drop=True), 'sm_points_fpga', DB)

    single_df_insert(stats.reset_index(), 'sm_stats', DB)
    single_df_insert(diagnostics, 'sm_diagnostics', DB)

    return


def split_fpga(points, point_meta):
    """
    Splits FPGA points since they conflict if stored with regular points (non unique between MoT segments)
    :param points:
    :param point_meta:
    :return:
    """

    points_gt0 = points.loc[points['point_id'] >= 0, :]
    points_fpga = points.loc[points['point_id'] < 0, :]
    points_fpga = pd.merge(points_fpga, point_meta[['point_id', 'segment_id']], on='point_id', how='inner')

    return points_gt0, points_fpga


def single_df_insert(df, name, DB):
    """
    This deviates slightly from update_postgres() but essentially does the same thing. Maybe think about refactoring
    """

    sql = "INSERT INTO <POSTGRES_SCHEMA_OUTPUT>.{name} ({c}) VALUES "
    sql = DB.replace_schemas(sql).format(name=name, c=', '.join(df.columns.values))
    sql = temptable_template(df, sql, DB)
    # Mogrify can't handle not a time values (NaT) so needs to replace them
    sql = sql.replace("'NaT'","NULL")
    DB.execute(sql)

    return


def truncate_all_sm_tables(DB):
    """
    Empties all tables used to store schedule matching output for visualization (sm_* tables)
    """

    sql = "TRUNCATE TABLE <POSTGRES_SCHEMA_OUTPUT>.{name};"
    sql = DB.replace_schemas(sql)

    DB.execute(sql.format(name='sm_trips'))
    DB.execute(sql.format(name='sm_trip_link'))
    DB.execute(sql.format(name='sm_itineraries'))
    DB.execute(sql.format(name='sm_legs'))
    DB.execute(sql.format(name='sm_segments'))

    DB.execute(sql.format(name='sm_points'))
    DB.execute(sql.format(name='sm_point_meta'))

    DB.execute(sql.format(name='sm_stats'))
    DB.execute(sql.format(name='sm_diagnostics'))

    return


def save_failed_trips(list_mot_id, trips, trip_link, DB):
    # Find out the missing trips
    x1 = np.array(list_mot_id)  # converts to numpy array
    x2 = trips['mot_segment_id'].unique()
    x3 = trip_link.reset_index()['mot_segment_id'].unique()

    no_se_station = x1[~np.in1d(x1, x2)]
    no_sbb_routing = x2[~np.in1d(x2, x3)]

    extraction_time = datetime.utcnow()

    content1 = [(idx, 'no start or end station found', extraction_time) for idx in no_se_station]
    content2 = [(idx, 'no routing possible (without visit overlap)', extraction_time) for idx in no_sbb_routing]

    failed_trips = pd.DataFrame(content1 + content2, columns=['mot_segment_id', 'failure_cause', 'datetime_created'])

    if failed_trips.empty:
        return

    update_postgres(failed_trips, 'train_trips_failed', DB)
    return


def clear_reprocessed_trips(train_trips, DB):
    sql_var = {'bfi': train_trips.bound_from_id.tolist()}
    sql_query = DB.get_query('clear_reprocessed_trips', __file__)
    DB.execute(sql_query, sql_var)

def update_train_trips_leg_with_delays(trips, DB):
    """
    update train_trips_leg table with any delay times found.
    """
    mot_segment_ids = list(trips['mot_segment_id'].unique())
    sql = DB.get_query('updatetraindelays', __file__)
    sql_var = {'mot_segment_ids': mot_segment_ids}
    DB.execute(sql,sql_var)
    return

