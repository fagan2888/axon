# -*- coding: utf-8 -*-
import pandas as pd
import threading
import Queue
import logging
import time

import init_data_struct as ids
from extract_from_xml import build_single_itinerary
from error_handling import eval_response
import trip as tr
import sbb_response

class SBBAPIThread(threading.Thread):
    def __init__(self, q_in, q_out, CONFIG):
        super(SBBAPIThread, self).__init__()
        self.q_in = q_in
        self.q_out = q_out
        self.CONFIG = CONFIG

    def run(self):
        while True:
            params = self.q_in.get()

            try:
                response = eval_response(params, self.CONFIG)
                if response:
                    self.q_out.put(response)
            except:
                logging.error('Worker failed to get response for params: {p}'.format(p=params))
            finally:
                self.q_in.task_done()


def api_call_thread(q_in, q_out, q_filled, CONFIG):

    # keep_alive = True
    # while keep_alive:
    #
    #     # Tries to get an item from the queue,
    #     try:
    #         params = q_in.get(True, timeout=5)
    #     except Queue.Empty:
    #         # Get returns an .Empty error when queue is empty
    #         if not q_filled.empty():
    #             # If all things to do are exhausted, kill worker
    #             keep_alive = False
    #         # Do not run code below if the queue was empty
    #         continue
    #
    #     # If a set of parameter was obtained from queue, make the API call
    #     try:
    #         # Calls the SBB API and returns the response message
    #         response = eval_response(params, CONFIG)
    #         # Skip operations for item if there is an error in the response (query_sbb_api() returns None)
    #         if response:
    #             q_out.put(response)
    #     except:
    #         logging.error('Worker failed to get response for params: {p}'.format(p=params))
    #     # Ensures that tasks gets flagged as done even if an error occurred so that bot doesn't hang
    #     finally:
    #         q_in.task_done()
    #
    # return

    while True:
        params = q_in.get()

        try:
            response = eval_response(params, CONFIG)
            if response:
                q_out.put(response)
        except:
            logging.error('Worker failed to get response for params: {p}'.format(p=params))
        finally:
            q_in.task_done()

# TODO switch to 'from vibepy.multithread_workers import MultiThreadOp'
def multithread_api_queries(trip, loop_through, CONFIG):

    num_fetch_threads = len(loop_through)
    q_in = Queue.Queue()  # Queue of inputs
    q_out = Queue.Queue()  # Queue of outputs
    q_filled = Queue.Queue() # When all inputs have been filled, a True is added to this queue ot tell workers to close

    # Initialize the threads
    for i in range(num_fetch_threads):
        # worker = threading.Thread(target=api_call_thread, args=(q_in, q_out, q_filled, CONFIG))
        worker = SBBAPIThread(q_in, q_out, CONFIG)
        worker.setDaemon(True)
        worker.start()

    # Populate queue
    for item in loop_through:
        # Generate a dictionary (params) that contains all the information required ot build the API call
        params = gen_param_seg(trip, MaxResultNumber=item[0], leave_at=item[1])
        q_in.put(params)

    # This is really stupid and in need of a better solution but it should prevents daemon threads from accumulating
    q_filled.put(True)
    # Wait for all workers to be done and return que of responses
    q_in.join()
    return q_out


def build_single_trip(trip, CONFIG):

    # 6 preceding/following departure/arrival
    loop_through = [(6, True), (6, False), (-6, True), (-6, False)]
    # Multi-threaded API calls (returns fifo queue of responses)
    q_out = multithread_api_queries(trip, loop_through, CONFIG)

    # Initialize empty dataframe with the right column structure for easy concatenation later
    # trip_link_df, itinerary_df, legs_df, segments_df = ids.initialize_all_empty_df()
    newTrip = tr.Trip(trip, CONFIG)

    while not q_out.empty():
        # Pop one response for processing
        response = q_out.get()

        # extracts the valuable information from the API query and stores it into pandas dataframes
        #  (This runs in about 0.04 seconds per response.content, not a bottleneck anymore)
        # trip_link_df_i, itinerary_df_i, legs_df_i, segments_df_i = \
        #     build_single_itinerary(response.content, trip, itinerary_df['context_reconstruction'].values, CONFIG)
        newTrip.build_single_itinerary(sbb_response.SBBResponse(response))

        # Update the dataframe with new items
        # we can do this later now
        # trip_link_df = pd.concat([trip_link_df, trip_link_df_i], ignore_index=True)
        # itinerary_df = pd.concat([itinerary_df, itinerary_df_i])
        # legs_df = pd.concat([legs_df, legs_df_i])
        # segments_df = pd.concat([segments_df, segments_df_i])

    newTrip.concat_trip_dfs()
    newTrip.concat_legs_dfs()
    newTrip.concat_seg_dfs()
    # Only perform these on non-empty dataframes
    # if not trip_link_df.empty:
    #     # These values are shared across all rows so add them at once
    #     trip_link_df['vid'] = trip['vid']
    #     trip_link_df['mot_segment_id'] = trip['mot_segment_id']
    #
    #     # Not using vid / mot_segment_id as indexes since they're identical for all...
    #     # Hierarchical indexes need to be sorted for faster operation (uses np.searchsorted )
    #     trip_link_df.sort_values(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)
    #
    #     trip_link_df.set_index(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)
    #
    # return trip_link_df, itinerary_df, legs_df, segments_df
    if not newTrip.trip_link_df.empty:
        # These values are shared across all rows so add them at once
        newTrip.trip_link_df['vid'] = trip['vid']
        newTrip.trip_link_df['mot_segment_id'] = trip['mot_segment_id']

        # Not using vid / mot_segment_id as indexes since they're identical for all...
        # Hierarchical indexes need to be sorted for faster operation (uses np.searchsorted )
        newTrip.trip_link_df.sort_values(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)

        newTrip.trip_link_df.set_index(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)

    return newTrip.trip_link_df, newTrip.itinerary_df, newTrip.legs_df, newTrip.segments_df


# def build_single_trip2(trip, CONFIG):
#     # 6 preceding/following departure/arrival
#     loop_through = [(6, True), (6, False), (-6, True), (-6, False)]
#
#     # Initialize empty dataframe with the right column structure for easy concatenation later
#     trip_link_df, itinerary_df, legs_df, segments_df = ids.initialize_all_empty_df()
#
#     for item in loop_through:
#         # Generate a dicitonary (params) that contains all the information required ot build the API call
#         params = gen_param_seg(trip, MaxResultNumber=item[0], leave_at=item[1])
#         # Calls the SBB API and returns the response message
#         response = eval_response(params, CONFIG)
#
#         # Skip operations for item if there is an error in the response (query_sbb_api() returns None)
#         if not response:
#             continue
#
#         # extracts the valuable information from the API query and stores it into pandas dataframes
#         #  (This runs in about 0.04 seconds per response.content, not a bottleneck anymore)
#         trip_link_df_i, itinerary_df_i, legs_df_i, segments_df_i = \
#             build_single_itinerary(response.content, trip, itinerary_df['context_reconstruction'].values, CONFIG)
#
#         # Update the dataframe with new items
#         trip_link_df = pd.concat([trip_link_df, trip_link_df_i], ignore_index=True)
#         itinerary_df = pd.concat([itinerary_df, itinerary_df_i])
#         legs_df = pd.concat([legs_df, legs_df_i])
#         segments_df = pd.concat([segments_df, segments_df_i])
#
#     # These values are shared across all rows so add them at once
#     trip_link_df['vid'] = trip['vid']
#     trip_link_df['mot_segment_id'] = trip['mot_segment_id']
#
#     # Not using vid / mot_segment_id as indexes since they're identical for all...
#     # Hierarchical indexes need to be sorted for faster operation (uses np.searchsorted )
#     trip_link_df.sort_values(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)
#     trip_link_df.set_index(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)
#
#     return trip_link_df, itinerary_df, legs_df, segments_df


def gen_param_seg(trip, MaxResultNumber=3, leave_at=True, api_version='v2'):
    # Some parameters need a bit of reformatting
    params = {
        'api_version': api_version,  # check, there might some other dependencies inside the XML...
        'MaxResultNumber': MaxResultNumber,
        'from_lat': int(round(trip['lat_start'] * 10 ** 6)),  # int, lat/lon * 1e6
        'from_lon': int(round(trip['lon_start'] * 10 ** 6)),
        'to_lat': int(round(trip['lat_end'] * 10 ** 6)),
        'to_lon': int(round(trip['lon_end'] * 10 ** 6)),

        # These times are used by python but not for the XML query so no reformat
        'trip_time_start': trip['trip_time_start'],  # previously starttriptime
        'trip_time_end': trip['trip_time_end']  # previously endtriptime
    }

    if leave_at:  # calculate trip to start at this time
        params['timestamp'] = trip['time_start'].strftime("%Y-%m-%dT%H:%M:%S")  # timestamp format: 2015-08-04T14:00:00
        params['DateTimeType'] = 'ABFAHRT'
    else:  # calculate trip to arrive by this time
        params['timestamp'] = trip['time_end'].strftime("%Y-%m-%dT%H:%M:%S")  # timestamp format: 2015-08-04T14:00:00
        params['DateTimeType'] = 'ANKUNFT'

    params['mot_segment_id'] = trip['mot_segment_id']

    return params
