import pandas as pd


def get_stops(list_mot_id, DB, CONFIG):
    """ Get start/end stops associated with each MoT ID and bounding trip times

    :param list_mot_id:
    :param DB: An instance of the PostgresManager class
    :param CONFIG: A loaded configuration from the application.ini file
    :return: trips - Pandas DataFrame with following columns:
    """

    mot_stops = find_nearest_stop(list_mot_id, DB, CONFIG)
    trip_times = get_trip_times(list_mot_id, DB, CONFIG)
    trips = pd.merge(mot_stops, trip_times, on='mot_segment_id')

    return trips


def get_trip_times(list_mot_id, DB, CONFIG):
    """
    Gets time of the last visit preceeding a MoT segment and first visit following it
    Adding the lat/lon for the start and end points.

    :param list_mot_id:
    :param DB: An instance of the PostgresManager class
    :return:
    """

    # sql_str_fname = 'event/getstops/sql/get_trip_times.sql'
    # data_bounds = pd_read_sql_from_file(list_mot_id, sql_str_fname, CONFIG)

    sql = DB.get_query('get_trip_times', __file__)
    sql_var = {'mot_id': tuple(list_mot_id),
               'min_visit_dur': CONFIG.getint('pointprocessing', 'MIN_VISIT_DURATION')}

    data_bounds = DB.postgres2pandas(sql, params=sql_var)

    return data_bounds


def find_nearest_stop(list_mot_id, DB, CONFIG):
    """
    Finds the nearest stops to the start and end points of each MoT segment stored in list_mot_id

    :param list_mot_id:
    :param DB: An instance of the PostgresManager class
    :param CONFIG: A loaded configuration from the application.ini file
    :return:
    """

    sql = DB.get_query('find_nearest_stop', __file__)
    sql_var = {'mot_id': list_mot_id,
               'dist_lim': int(CONFIG.get('params', 'MAX_WALK_DIST'))}

    data = DB.postgres2pandas(sql, params=sql_var)

    # Drop trips where the start and end station is identical because it causes OTP to crash
    data = data[(data['lat_start'] != data['lat_end']) & (data['lon_start'] != data['lon_end'])]

    data.sort_values('time_start', inplace=True)
    data.reset_index(inplace=True, drop=True)

    return data
