import xml_path
import uuid
from numpy import logical_and
import pandas as pd
from datetime import timedelta

import init_data_struct as ids
# all eval_ methods


def eval_leg_type_error(leg):

    leg_type = xml_path.get_leg_type(leg)
    if leg_type == 'OEV':

        # previously get_leg_route_category(), shoiuld be identical (todo check!)
        leg_subtype = xml_path.get_leg_route_category(leg)

        if leg_subtype in ['bus', 'nfb']:
            # bus, skip segments but keep leg
            leg_type_anomaly = 'OEV - BUS ({t})'.format(t=leg_subtype)

        elif leg_subtype in ['t', 'nft']:
            # tram, skip segments but keep leg
            leg_type_anomaly = 'OEV - TRAM ({t})'.format(t=leg_subtype)

        else:
            leg_type_anomaly = ''

    else:
        leg_type_anomaly = leg_type

    return leg_type_anomaly


def eval_leg_route_name(legs_df):

    # initialize and ensures str() rather than NaN-float
    legs_df['route_name'] = legs_df['route_category'] + ' '
    # unlike previous version of code, this is a Null value and not a 'None' str
    mask = legs_df['route_line'].isnull()
    legs_df.ix[mask, 'route_name'] += legs_df.ix[mask, 'route_number']
    legs_df.ix[~mask, 'route_name'] += legs_df.ix[~mask, 'route_line']
    return


def eval_nat_replace(segments_df):

    # set start time to end time if missing
    segments_df.loc[segments_df['time_start'].isnull(), 'time_start'] = \
        segments_df.loc[segments_df['time_start'].isnull(), 'time_end']

    # set end time to start time if missing
    segments_df.loc[segments_df['time_end'].isnull(), 'time_end'] = \
        segments_df.loc[segments_df['time_end'].isnull(), 'time_start']

    return


def eval_segment_is_waypoint(segments_df):

    segments_df['waypoint'] = segments_df['node'].apply(xml_path.get_seg_type) != 'STATION'
    segments_df.loc[(segments_df['time_start'].isnull() & segments_df['time_end'].isnull()), 'waypoint'] = True

    return


def add_moving_segments(segments_df, legs_df, trip_link_df, CONFIG):
    """
    Segments are only the static stays at the station, and their IDs are all even.
    Adds odd-numbered movements segments that link station stops.
    Updates the trip_link_df with the new IDs

    :param segments_df:
    :param legs_df:
    :param trip_link_df:
    :param CONFIG:
    :return:
    """

    # TODO test that waypoint inclusion works well

    leg_subset = legs_df.loc[legs_df['leg_type'] == '', ['leg_number']]
    seg_subset = segments_df.loc[~segments_df['waypoint'],
                                 ['segment_number', 'time_start', 'time_end', 'stop_id_start', 'stop_id_end']]

    merged = pd.merge(trip_link_df, leg_subset, left_on='leg_id', right_index=True, suffixes=('', '_leg'), sort=False)
    merged = pd.merge(merged, seg_subset, left_on='segment_id', right_index=True, suffixes=('', '_seg'), sort=False)

    # values need to be ordered before using .shift()
    merged.sort_values(['itinerary_id', 'leg_number', 'segment_number'], ascending=True, inplace=True)

    # Pads with START_TRIP_BUFFER the 1st and last segment to include the wait at station.
    time_buffer = timedelta(seconds=int(CONFIG.get('params', 'START_TRIP_BUFFER')))
    merged_groupby = merged.copy().groupby('itinerary_id')  # TODO -- why is COPY needed?
    first_pts_list = merged_groupby['segment_id'].first()
    segments_df.loc[first_pts_list.values, 'time_start'] = segments_df.loc[first_pts_list.values, 'time_end']\
        - time_buffer
    last_pts_list = merged_groupby['segment_id'].last()
    segments_df.loc[last_pts_list.values, 'time_end'] = segments_df.loc[last_pts_list.values, 'time_start'] \
        + time_buffer

    # starts from the end of previous segment and goes to start of next one
    temp_col_names = {'time_end': 'time_start',
                      'stop_id_end': 'stop_id_start',
                      'time_start': 'time_end',
                      'stop_id_start': 'stop_id_end'
                      }
    merged.rename(columns=temp_col_names, inplace=True)

    merged[['time_end', 'stop_id_end']] = merged[['time_end', 'stop_id_end']].shift(-1).values
    merged['segment_number'] += 1

    # Drop segments that link different itineraries
    merged = merged[merged['itinerary_id'] == merged['itinerary_id'].shift(-1)]
    # Initialize new uuid for the segments that were created
    merged['segment_id'] = [str(uuid.uuid4()) for i in range(merged['segment_id'].shape[0])]
    merged['waypoint'] = False

    new_seg_view = merged[['segment_id', 'segment_number', 'time_start', 'time_end', 'stop_id_start', 'stop_id_end',
                           'waypoint']]
    new_segments = ids.init_segments_df(values=new_seg_view, set_index=True, drop_node=True)

    segments_df = pd.concat([segments_df, new_segments])
    trip_link_df = pd.concat([trip_link_df, merged[trip_link_df.columns]])

    # Identify long_pause segments
    # # (these are weighted more heavily later because 'static' points are deemed more reliable)
    train_long_stop_threshold = timedelta(seconds=int(CONFIG.get('params', 'TRAIN_LONG_STOP_THRESHOLD')))
    segments_df['is_long_stop'] = logical_and(
        (segments_df['time_end'] - segments_df['time_start']) >= train_long_stop_threshold,
        (segments_df['segment_number'] % 2) == 0)

    return segments_df, trip_link_df


def eval_n_leg(trip_link_df, itinerary_df, legs_df):
    """
    This currently calculates all legs, including those labeled as 'FUSSWEG' ot other ones to be skipped...

    :param trip_link_df:
    :param itinerary_df:
    :param legs_df:
    :return:
    """

    # takes only legs that are well conditioned
    leg_mask = trip_link_df['leg_id'].isin(legs_df[legs_df['leg_type'] == ''].index)
    # count legs
    leg_count = trip_link_df.loc[leg_mask, ['itinerary_id', 'leg_id']].groupby('itinerary_id')['leg_id'].nunique()
    itinerary_df['num_legs'] = leg_count
    # Itineraries that have 0 legs won't get counted to fill NaN with 0
    itinerary_df['num_legs'].fillna(0, inplace=True)

    return


def eval_n_seg(trip_link_df, legs_df, segments_df, seg_count_name):

    # Takes only segments that are not way points
    seg_mask = trip_link_df['segment_id'].isin(segments_df[~segments_df['waypoint']].index)
    # Count segments
    seg_count = trip_link_df.loc[seg_mask, ['leg_id', 'segment_id']].groupby('leg_id').count()
    seg_count.rename(columns={'segment_id': 'num_legs'}, inplace=True)
    legs_df[seg_count_name] = seg_count
    # Legs that have 0 segments won't get counted to fill NaN with 0
    legs_df[seg_count_name].fillna(0, inplace=True)

    return
