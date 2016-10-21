import numpy as np
import pandas as pd

try:
    from ..dbutil.temptable_template import temptable_template
except:
    from dbutil.temptable_template import temptable_template

def order_filter(trips, legs, points, trip_link, point_meta, DB, CONFIG):
    """
    Given the dataframes describing trips, and the associated points, this function determines whether a point should be
    tagged as out of order. The SQL statement snapes the point to the candidate train route, then determines the spatial
    order, and the temporal order of these points. If there is too much disagreement (as measured by a comparison to the
    standard deviation of the out of order) it is flagged.

    :param trips: The trips dataframe
    :param legs: The legs dataframe
    :param points: The points dataframe
    :param trip_link: The trip_link dataframe
    :param point_meta: The point_meta (obviously without the order_filter column)
    :param conn: A database connection
    :param CONFIG: A parsed config file pointing to the relevant application.ini
    :return: Returns a dataframe containin the itinerary_id, segment_id, point_id, and the outlier flag (boolean)
    """

    ORDER_FILTER_CUTOFF = CONFIG.getfloat('params', 'ORDER_FILTER_CUTOFF')
    ORDER_FILTER_MIN_STD = CONFIG.getfloat('params', 'ORDER_FILTER_MIN_STD')

    legs = legs.reset_index()

    # Join all the dfs together to make a super dataframe from which I can get my data!
    points_joined = legs.merge(trip_link.reset_index(), on='leg_id')
    points_joined = points_joined.merge(point_meta.reset_index(), on='segment_id')
    points_joined = points_joined.merge(points.reset_index(), on='point_id')
    points_joined = points_joined.merge(trips.reset_index(), on='mot_segment_id', suffixes=('_legs', '_trips'))

    sql = DB.get_query('temporaryooo', __file__)
    sql = temptable_template(points_joined[['itinerary_id', 'leg_id', 'segment_id', 'point_id', 'route_name', 'agency_id',
                                            'stop_id_start_legs', 'stop_id_end_legs', 'time', 'lat', 'lon']], sql, DB)
    sql += DB.get_query('getgeoms', __file__)

    # points_joined.to_excel('2.xlsx')
    df = DB.postgres2pandas(sql)

    # We first define how different the data order is from the time order (i.e. the true order)
    df['orderdiff'] = (df['data_order'] - df['time_order']).abs()

    # df.sort(columns=['point_id','time_order','data_order']).to_excel('2.xlsx')
    if df.empty:
        grouped_df = df.astype(float).groupby('leg_id')
    else:
        grouped_df = df.groupby('leg_id')

    # Calculate the stats for this trip, how out of order is the average point?
    itinerary_stats = grouped_df['orderdiff'].agg({'orderdiff_mean': np.mean, 'orderdiff_std': np.std}).reset_index()

    # print itinerary_stats
    df = df.merge(itinerary_stats, on='leg_id')
    # We don't want the standard deviation to be too small or most points could get flagged as out of order. Make it a
    # minimum of ORDER_FILTER_MIN_STD (at the time of coding, this is 2)
    df.ix[df['orderdiff_std'] < ORDER_FILTER_MIN_STD, 'orderdiff_std'] = ORDER_FILTER_MIN_STD

    # How many standard deviations out of order is this point? If it's greater than ORDER_FILTER_CUTOFF then flag it
    # as an ooo outlier by setting ooo_outlier=True
    df['ooo_outlier'] = ((df.orderdiff - df.orderdiff_mean) / df.orderdiff_std) >= ORDER_FILTER_CUTOFF

    return df[['itinerary_id', 'segment_id',  'point_id', 'ooo_outlier']]
