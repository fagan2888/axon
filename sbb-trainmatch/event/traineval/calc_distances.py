import numpy as np
import logging

try:
    from ..dbutil.temptable_template import temptable_template
    from ..filters.order import order_filter
except:
    from dbutil.temptable_template import temptable_template
    from filters.order import order_filter

def calc_distances(trips, itineraries, legs, segments, trip_link, DB, CONFIG):
    """
    Given dataframes describing a bunch of candidate trips, this function looks up the distances of points that overlap
    these trips to the railroad that they likely travel on. This goes to the SQL database to get these distance.

    :param trips: Trips dataframe
    :param itineraries: Itineraries dataframe
    :param legs: Legs dataframe
    :param segments: Segments dataframe
    :param trip_link: Trip_link dataframe
    :param DB: An instance of the PostgresManager class
    :param CONFIG: A parsed config file pointing to the relevant application.ini
    :return: Returns the points and point_meta dataframes
    """

    segments_joined = segments.join(trip_link, how='inner')

    segments_joined = segments_joined.join(legs[['route_name', 'agency_id', 'num_segments', 'leg_number']], how='inner')

    segments_joined = segments_joined.join(itineraries, rsuffix='_itineraries', how='inner')

    # This is going to subtract a time interval from the start of each trip and add the same interval to the end of each
    # trip.
    # segments_joined = buffer_startend_time(segments_joined, CONFIG) # Now done after the SBB API CALL

    # What we do here is add all the route information to a temporary table (see the temporarysegments query),
    # run the distance query which does the requisite joins to calculate the distances to the correct train route geom.
    sql = DB.get_query('temporarysegments', __file__)
    sql = temptable_template(segments_joined.reset_index()[
                                 ['segment_id', 'vid', 'route_name', 'agency_id', 'time_start', 'time_end',
                                  'stop_id_start', 'stop_id_end']], sql, DB)
    sql += DB.get_query('distance', __file__)

    distances = DB.postgres2pandas(sql)

    points = distances.merge(trip_link.reset_index(), on='segment_id')
    points = points.merge(trips.reset_index(), on='mot_segment_id')

    points['within_mot_segment'] = ((points['time']>=points['time_start']) & (points['time']<=points['time_end']))

    point_meta = points[['point_id', 'segment_id', 'distance']]

    points = points[['point_id', 'lat', 'lon', 'time', 'horizontal_accuracy', 'within_mot_segment']]

    points.drop_duplicates(inplace=True)
    point_meta.drop_duplicates(inplace=True)

    points.set_index('point_id', inplace=True)

    # If we don't have any points that overlap, return an empty dataframe before the ooo filter.
    if point_meta.empty:
        logging.warning("""No points found that occur within the itineraries.
                         This probably shouldn't happen, check your data.""")
        return points, point_meta

    order_filter_points = order_filter(trips, legs, points, trip_link, point_meta, DB, CONFIG)

    point_meta = point_meta.merge(order_filter_points, on=['point_id', 'segment_id'], how='left')
    point_meta = point_meta.merge(segments[['is_long_stop']], left_on='segment_id', right_index=True, how='left')

    point_meta = point_meta[['point_id', 'segment_id', 'distance', 'ooo_outlier','is_long_stop']]
    
    point_meta.fillna(False, inplace=True)
    point_meta.set_index(['segment_id', 'point_id'], inplace=True)

    return points, point_meta


# def buffer_startend_time(segments_joined, CONFIG):
#     """
#     This function subtracts (adds) START_TRIP_BUFFER seconds from the start (end) of each itinerary.
#
#     :param segments_joined: This is a dataframe which joins the legs, segments and itineraries datafarmes
#     :param CONFIG: A parsed config file pointing to the relevant application.ini
#     :return: Returns segments_joined, but with the start times of the itineraries buffered
#     """
#
#     START_TRIP_BUFFER = CONFIG.get('params', 'START_TRIP_BUFFER')
#
#     segments_joined.ix[(segments_joined.segment_number == 0) & (segments_joined.leg_number == 0), 'time_start'] = \
#         segments_joined.ix[(segments_joined.segment_number == 0) & (segments_joined.leg_number == 0), 'time_start'] - \
#         np.timedelta64(START_TRIP_BUFFER, 's')
#
#     segments_joined.ix[(segments_joined.segment_number == segments_joined.num_segments - 1) &
#                        (segments_joined.leg_number == segments_joined.num_legs - 1), 'time_end'] = \
#         segments_joined.ix[(segments_joined.segment_number == segments_joined.num_segments - 1) &
#                            (segments_joined.leg_number == segments_joined.num_legs - 1), 'time_end'] + \
#         np.timedelta64(START_TRIP_BUFFER, 's')
#
#     return segments_joined