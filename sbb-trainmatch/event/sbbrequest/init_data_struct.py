import pandas as pd


def init_df(col_names, values=None):
    """
    Initialize a generic dataframe with columns col_names, empty unless values are supplies.
    :param col_names:
    :param values:
    :return:
    """

    if values is not None:
        df = pd.DataFrame(values, columns=col_names)
    else:
        df = pd.DataFrame(columns=col_names)

    return df


def init_legs_df(values=None, drop_node=False, set_index=False):

    col_names = [
        'leg_id',
        'node',
        'leg_number',
        'route_full_name',
        'route_category',
        'route_line',
        'route_number',
        'agency_id',
        'num_segments',
        'time_start',
        'time_planned_start',
        'stop_id_start',
        'platform_start',
        'station_name_start',
        'time_end',
        'time_planned_end',
        'stop_id_end',
        'platform_end',
        'station_name_end',
        'route_name',
        'nb_train_stops',
        'leg_type']

    df = init_df(col_names, values=values)

    if set_index:
        df.set_index('leg_id', inplace=True)
    if drop_node:
        df.drop('node', axis=1, inplace=True)

    return df


def init_segments_df(values=None, drop_node=False, set_index=False):

    col_names = [
        'segment_id',
        'node',
        'segment_number',
        'time_start',
        'time_end',
        'stop_id_start',
        'stop_id_end',
        'waypoint'
        ]

    df = init_df(col_names, values=values)

    if set_index:
        df.set_index('segment_id', inplace=True)
    if drop_node:
        df.drop('node', axis=1, inplace=True)

    return df


def init_trip_link_df(values=None):

    col_names = [
        'vid',
        'mot_segment_id',
        'itinerary_id',
        'leg_id',
        'segment_id'
        ]

    df = init_df(col_names, values=values)

    return df


def init_itineraries_df(values=None, drop_node=False, set_index=False):

    col_names = [
        'itinerary_id',
        'node',
        'time_start',
        'time_end',
        'context_reconstruction',
        'num_legs'
        ]

    df = init_df(col_names, values=values)

    if set_index:
        df.set_index('itinerary_id', inplace=True)
    if drop_node:
        df.drop('node', axis=1, inplace=True)

    return df


def initialize_all_empty_df():
    """
    Initializes multiple dataframes that will be concatenated later

    :return: trip_link_df, itinerary_df, legs_df, segments_df
    """

    itinerary_df = init_itineraries_df(drop_node=True, set_index=True)
    legs_df = init_legs_df(drop_node=True, set_index=True)
    segments_df = init_segments_df(drop_node=True, set_index=True)
    trip_link_df = init_trip_link_df()

    return trip_link_df, itinerary_df, legs_df, segments_df
