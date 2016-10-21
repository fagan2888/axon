import multiprocessing
import logging
import pandas as pd
from joblib import Parallel, delayed

from getstops import getstops
from sbbrequest.build_single_trip import build_single_trip
from traineval.calc_distances import calc_distances
from traineval.fpga import fpga
from traineval.eval_itin_quality import get_best_itinerary
from traineval.output_to_postgres import save_output, save_failed_trips

from metrics.metrics import write_metrics
from vibepy.load_logger import TimeLogger

import sbbrequest.init_data_struct as ids


def process_batch(list_mot_id, CONFIG, DB):
    """
    Processes a batch of mot_segment IDs that share a common batch_id

    Calls the SBB API for routing between start/end stations
    Writes metrics to grafana
    Saves the shortened output to postgres in train_trips / train_trips_leg tables
    All the dataframes are stored in postgres as sm_XXXXXX tables for visualization

    :param batch_id: integer
    :param list_mot_id: list of string
    :param CONFIG: configuration loaded from application.ini
    :param DB: postgres database object for connections
    :return:
    """

    logging.info('Processing MoT segments: {s}'.format(s=list_mot_id))

    # Just timing execution time for logs
    time_log = TimeLogger()

    # Skip if no MoT segments returned...
    if not list_mot_id:
        return None

    # Get the start/end stops associated with each MoT segment (aka trip)
    trips = getstops.get_stops(list_mot_id, DB, CONFIG)
    time_log.log_runtime(msg='Get Stops. ')

    # Pandas-style iterations over row running in parallel using all available processors on the machine
    trip_link, itineraries, legs, segments = temp_fix_apply(trips, CONFIG)
    # trip_link, itineraries, legs, segments = apply_parallel(trips, build_single_trip, CONFIG)
    # trip_link, itineraries, legs, segments = apply_multithread(trips, CONFIG)

    # Escapes if no trips are returned (SBB API can't output valid routes between any of the start/end station/time
    if trip_link.shape[0] == 0:
        msg = 'No Itineraries returned for any of the MoT Segments IDs. MoT_IDs={ids}'
        logging.warning(msg.format(ids=list_mot_id))
        save_failed_trips(list_mot_id, trips, trip_link, DB)
        return

    # Order the table for faster indexed searches in pandas
    ordered_col_list = ['vid', 'mot_segment_id', 'itinerary_id', 'leg_id', 'segment_id']
    trip_link = trip_link.reset_index().sort_values(ordered_col_list).set_index(ordered_col_list)
    time_log.log_runtime(msg='Apply parallel. ')

    # For some stupid reason pandas inverts boolean into (-1,0) integers rather than the inverse boolean...
    points, point_meta = calc_distances(trips, itineraries, legs[legs['leg_type'] == ''],
                                        segments[segments['waypoint'] == False], trip_link, DB, CONFIG)
    time_log.log_runtime(msg='Calc Distance. ')
    # TODO check if empty + skip?

    # Adds points at start and end of each leg, so that legs which don't overlap with any data don't trick the code
    points, point_meta = fpga(points, point_meta, legs[legs['leg_type'] == ''],
                              segments[segments['segment_number'] == 0], trip_link, DB)
    # FPGA points should be stored separately as enforcing unicity between segments is not enforced

    # Builds the diagnostics and evaluate best itinerary
    stats, diagnostics = get_best_itinerary(trip_link, points, point_meta, CONFIG)
    time_log.log_runtime(msg='Get Best Itinerary. ')

    # Stores metrics in grafana
    write_metrics(list_mot_id, trip_link, trips, stats, point_meta, points, CONFIG)

    # Save all the required outputs (both sm_ tables and train_trips/train_trips_leg
    save_output(trip_link, trips, itineraries, segments, legs, points, point_meta, stats, diagnostics, DB)
    save_failed_trips(list_mot_id, trips, trip_link, DB)
    time_log.log_runtime(msg='Update postgres. ')

    return


def temp_fix_apply(trips, CONFIG):

    trip_link, itineraries, legs, segments = ids.initialize_all_empty_df()
    trip_link.set_index(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)

    for index, trip in trips.iterrows():
        trip_link_i, itinerary_i, legs_i, segments_i = build_single_trip(trip, CONFIG)

        trip_link = pd.concat([trip_link, trip_link_i])
        itineraries = pd.concat([itineraries, itinerary_i])
        legs = pd.concat([legs, legs_i])
        segments = pd.concat([segments, segments_i])

    return trip_link, itineraries, legs, segments


# Multi-threaded
# from vibepy.multithread_workers import MultiThreadOp
#
# class BuildTrips(MultiThreadOp):
#     def worker_target_fun(self, item):
#         # Overloads the place-holder method
#         trip_link_i, itinerary_i, legs_i, segments_i = build_single_trip(item, self.CONFIG)
#         out = (trip_link_i, itinerary_i, legs_i, segments_i)
#         self.q_out.put(out)
#         return
#
# def apply_multithread(trips, CONFIG):
#     n_workers = 10
#     items = [trip for trip in trips.iterrows()] # TODO : NOT TESTED!!! Just added so new module is used
#     my_test = BuildTrips(n_workers=n_workers, config=CONFIG)
#     q_out = my_test.run(items)
#
#     trip_link, itineraries, legs, segments = ids.initialize_all_empty_df()
#     trip_link.set_index(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)
#
#     while not q_out.empty():
#         (trip_link_i, itinerary_i, legs_i, segments_i) = q_out.get()
#
#         trip_link = pd.concat([trip_link, trip_link_i])
#         itineraries = pd.concat([itineraries, itinerary_i])
#         legs = pd.concat([legs, legs_i])
#         segments = pd.concat([segments, segments_i])
#
#     return trip_link, itineraries, legs, segments


def apply_parallel(df, func, CONFIG):
    """
    Longest function description ever for a 2 line function

    iterates through the rows of df and apply func with parameter CONFIG to each row individually.
    The is performed with the multiprocessing module / joblib

    Notes on parallel/delayed.
        The delayed(func)(values) is decorator used to capture the arguments of a function
        The operation being performed in parallel is therefore func(values)

    returns an unpacked, concatenated version of the output e.g. parallel returns a list of tuples of outputs
    e.g. [(outA_1, outB_1, outC_1), (outA_2, outB_2, outC_2) , ...]
    with each output in the tuple a dataframe
    this concatenates elements belonging to the same dataframe so that the output of the called function is matched
    e.g. outA = [outA_1+outA_2+outA_3+...], outB = [outA_1+outB_2+outB_3+...], outC = [outC_1+outC_2+outC_3+...]

    For some documentation on multiprocessing in pandas
    # https://docs.python.org/2/library/multiprocessing.html#module-multiprocessing
    # https://gist.github.com/yong27/7869662
    # http://stackoverflow.com/questions/26187759/parallelize-apply-after-pandas-groupby/27027632#27027632
    The selected method performed slightly better than the others but was mainly chosen for simplicity of writing
    """

    # Because the contortions that @dfo had to go through to get Luigi+Bot working preclude the use of the multi-
    # processing library we need to set n=1 here

    # n = multiprocessing.cpu_count()
    n = 4
    ret_list = Parallel(n_jobs=n)(delayed(func)(row, CONFIG) for index, row in df.iterrows())
    return tuple(pd.concat([x[i] for x in ret_list]) for i in range(len(ret_list[0])))
