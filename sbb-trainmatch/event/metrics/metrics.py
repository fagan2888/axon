import logging
import numpy as np

from vibepy.write_grafana import write_grafana


def write_metrics(list_mot_id, trip_link, trips, stats, point_meta, points, CONFIG):

    error_msg = [r'WARNING -- Low Counts',
                 r'WARNING -- High Avg. Distance',
                 r'WARNING -- Low overlap with MoT segment']

    metrics = {
        # total number fed in that batch
        'mot_total_input': len(list_mot_id),
        # of which had a start and end station in CH
        'mot_with_se_stn': trips.shape[0],
        # of which thr SBB routing engine found at least 1 itin
        'mot_with_itin': trip_link.reset_index()['mot_segment_id'].nunique(),
        'mot_no_warnings': stats.loc[~stats['warning_bool'], 'warning_bool'].count(),
        'mot_nw_unambiguous': stats.loc[np.logical_and(~stats['warning_bool'],
                                                       np.logical_or(stats['delta_next'] < 0,
                                                                     stats['delta_next'] > 3000)),
                                        'delta_next'].count(),
        # Breakdown of messages with warnings
        'mot_w_total': stats.loc[stats['warning_bool'], 'warning_bool'].count(),
        'mot_w_low_cnt': stats['warning_str'].str.contains(error_msg[0]).astype(int).sum(),
        'mot_w_high_dist': stats['warning_str'].str.contains(error_msg[1]).astype(int).sum(),
        'mot_w_low_overlap': stats['warning_str'].str.contains(error_msg[2]).astype(int).sum()
    }

    write_grafana(CONFIG, metrics, output_type='incr')

    # for stats on the properties, median of: stats['count', 'n_time_in', 'min_value']
    x = point_meta['ooo_outlier'].astype(int).sum() / point_meta['ooo_outlier'].count().astype(float)
    logging.info('STATS: ooo outlier fraction: {x}'.format(x=x))

    x = points['within_mot_segment'].astype(int).sum() / points['within_mot_segment'].count().astype(float)
    logging.info('STATS: n_times_in avg: {x}'.format(x=x))

    return
