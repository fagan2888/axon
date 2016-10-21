import pandas as pd
import numpy as np
import logging

from geopy.distance import vincenty


# Fake Point Generation Algorithm
def fpga(points, point_meta, legs, segments, trip_link, DB, n=3):
    # Using the leg start/end ID --> SQL query, get dist from station lat/lon for all n pts per leg start/end

    n_legs = legs.shape[0]  # Just to sanity check later the joins

    stop_id_list = {'stop_id': legs[['stop_id_start', 'stop_id_end']].stack().drop_duplicates().values.tolist()}
    sql = DB.get_query('stops_position', __file__)
    stops_position = DB.postgres2pandas(sql, params=stop_id_list)

    # legs[['leg_id', 'time_start', 'time_end', 'stop_id_start', 'stop_id_end']]
    # Need segment ID to update point_meta, and add new point id -> max(point_id) + constant
    new_col_name = {'leg_id': 'leg_id', 'time_start': 'time', 'time_end': 'time', 'stop_id_start': 'stop_id',
                    'stop_id_end': 'stop_id'}
    reformated_legs = pd.concat([legs[['time_start', 'stop_id_start']].reset_index().rename(columns=new_col_name),
                                 legs[['time_end', 'stop_id_end']].reset_index().rename(columns=new_col_name)],
                                ignore_index=True, axis=0)
    reformated_legs = pd.merge(reformated_legs, stops_position, left_on='stop_id', right_on='stop_id')
    reformated_legs[['lat', 'lon', 'distance']] = reformated_legs.apply(lambda x: approx_edge_position(x, points), axis=1)
    # Create point entries
    # points -- Point_id, lat, lon, time, horizontal_accuracy, within_mot_segment
    # point_meta -- segment_id, point_id, distance, ooo_outlier, is_long_stop
    reformated_legs['horizontal_accuracy'] = 10
    reformated_legs['within_mot_segment'] = False
    reformated_legs['ooo_outlier'] = False
    reformated_legs['is_long_stop'] = True
    reformated_legs['point_id'] = -(points.index.max() + 1 + reformated_legs.index)  #negative IDs to identify them
    # Add segment IDs
    leg_seg_link = pd.merge(trip_link.reset_index(level=(0, 1, 2), drop=True).reset_index().dropna(),
                            segments.reset_index()[['segment_id']], on='segment_id')

    reformated_legs = pd.merge(reformated_legs, leg_seg_link, on='leg_id')

    # Append new points
    pts_col_names = ['point_id', 'lat', 'lon', 'time', 'horizontal_accuracy', 'within_mot_segment']
    new_points = reformated_legs[pts_col_names].set_index('point_id', drop=True)
    points = pd.concat([new_points, points])

    # Append new points meta information
    pmeta_col_names = ['segment_id', 'point_id', 'distance', 'ooo_outlier', 'is_long_stop']
    new_point_meta = reformated_legs[pmeta_col_names].set_index(['segment_id', 'point_id'], drop=True)
    point_meta = pd.concat([new_point_meta, point_meta])

    # Just in case some weird stuff happens in the joins (has previously happened)
    if new_points.shape[0] != (2 * n_legs):
        logging.error('fpga : new points shape has {x} items but there are {n} legs'.format(x=new_points.shape[0], n=n_legs))
    if new_point_meta.shape[0] != (2 * n_legs):
        logging.error('fpga : new point_meta shape has {x} items but there are {n} legs'.format(x=new_points.shape[0], n=n_legs))

    return points, point_meta


def approx_edge_position(x, points, n=3):
    # Initial selection of only the n nearest point in time
    points_subset = points.iloc[np.abs(points['time'] - x['time']).argsort().iloc[:n],:]
    # Select best point of the subset using selection_criterion()
    xy = points_subset.iloc[points_subset.apply(lambda p: selection_criterion(x, p), axis=1).argsort().iloc[0], :]
    # Need to return a serie for the .apply() method calling the .iloc[0] function
    xy = xy[['lat','lon']]
    # In case selection criterion is not exclusively distance
    xy['distance'] = vincenty((x['lat'], x['lon']), (xy['lat'], xy['lon'])).meters
    return xy


def selection_criterion(x, p):
    # t = np.abs(p['time'] - x['time'])
    y = vincenty((x['lat'], x['lon']), (p['lat'], p['lon'])).meters  # Vincenty is a more precise great circle calc.
    # print (t,t/np.timedelta64(1,'h')*speed,y)
    return y
