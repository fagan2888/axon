# std python imports
import logging

# sci imports
import pandas as pd

# Vibe imports
from vibepy.load_logger import TimeLogger

# SBB imports
from getstops import getstops
from traineval.calc_distances import calc_distances
from traineval.fpga import fpga
from traineval.eval_itin_quality import get_best_itinerary
from traineval.output_to_postgres import save_output, save_failed_trips
from metrics.metrics import write_metrics
from sbbrequest.trip import Trip
import sbbrequest.init_data_struct as ids



class Batch(object):
    def __init__(self, batch_id, list_mod_id, loc_bounds, CONFIG, DB):
        self.batch_id = batch_id
        self.list_mot_id = list_mod_id
        self.loc_bounds = loc_bounds
        self.CONFIG = CONFIG
        self.DB = DB

        logging.info('Processing batch ID: {b}, including MoT segments: {s}'.format(b=self.batch_id, s=self.list_mot_id))

        # Just timing execution time for logs
        self.time_log = TimeLogger()

        # Get the start/end stops associated with each MoT segment (aka trip)
        self.trips = getstops.get_stops(self.list_mot_id, self.DB, self.CONFIG)
        self.time_log.log_runtime(msg='Get Stops. ({n} found for {bid})'.format(bid=self.batch_id, n=self.trips.shape[0]))
        self.trip_objs = dict()
        self.trips_processed = 0

    def init_trips(self):
        for _, trip in self.trips.iterrows():
            t = Trip(trip, self.batch_id, self.CONFIG)
            self.trip_objs[t.trip_id] = t

    def send_trip_requests(self):
        for t in self.trip_objs.itervalues():
            t.publish_reqs()

    def process_trips(self):
        trip_link, itineraries, legs, segments = self.build_trip_dfs()

        # Escapes if no trips are returned (SBB API can't output valid routes between any of the start/end station/time
        if trip_link.shape[0] == 0:
            msg = 'No Itineraries returned for any of the MoT Segments IDs. Batch_ID={bi}. MoT_IDs={ids}'
            logging.warning(msg.format(bi=self.batch_id, ids=self.list_mot_id))
            save_failed_trips(self.list_mot_id, self.trips, trip_link, self.DB)
            return

        # Order the table for faster indexed searches in pandas
        ordered_col_list = ['vid', 'mot_segment_id', 'itinerary_id', 'leg_id', 'segment_id']
        trip_link = trip_link.reset_index().sort_values(ordered_col_list).set_index(ordered_col_list)
        self.time_log.log_runtime(msg='Apply parallel. ({bid})'.format(bid=self.batch_id))

        # For some stupid reason pandas inverts boolean into (-1,0) integers rather than the inverse boolean...
        points, point_meta = calc_distances(self.trips, itineraries, legs[legs['leg_type'] == ''],
                                            segments[segments['waypoint'] == False], trip_link, self.DB, self.CONFIG)
        if trip_link.shape[0] == 0:
            save_failed_trips(self.list_mot_id, self.trips, trip_link, self.DB)
            return
        self.time_log.log_runtime(msg='Calc Distance. ({bid})'.format(bid=self.batch_id))

        # Adds points at start and end of each leg, so that legs which don't overlap with any data don't trick the code
        points, point_meta = fpga(points, point_meta, legs[legs['leg_type'] == ''],
                                  segments[segments['segment_number'] == 0], trip_link, self.DB)
        # FPGA points should be stored separately as enforcing unicity between segments is not enforced

        # Builds the diagnostics and evaluate best itinerary
        stats, diagnostics = get_best_itinerary(trip_link, points, point_meta, self.CONFIG)
        self.time_log.log_runtime(msg='Get Best Itinerary. ({bid})'.format(bid=self.batch_id))

        # Stores metrics in grafana
        write_metrics(self.list_mot_id, trip_link, self.trips, stats, point_meta, points, self.CONFIG)

        # Save all the required outputs (both sm_ tables and train_trips/train_trips_leg
        save_output(trip_link, self.trips, itineraries, segments, legs, points, point_meta, stats, diagnostics,
                    self.loc_bounds, self.DB)
        save_failed_trips(self.list_mot_id, self.trips, trip_link, self.DB)
        self.time_log.log_runtime(msg='Update postgres. ({bid})'.format(bid=self.batch_id))


    def build_trip_dfs(self):
        trip_link, itineraries, legs, segments = ids.initialize_all_empty_df()
        trip_link.set_index(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)

        for t in self.trip_objs.itervalues():
            trip_link_i, itinerary_i, legs_i, segments_i = t.trip_link_df, t.itinerary_df, t.legs_df, t.segments_df
            trip_link = pd.concat([trip_link, trip_link_i])
            itineraries = pd.concat([itineraries, itinerary_i])
            legs = pd.concat([legs, legs_i])
            segments = pd.concat([segments, segments_i])

        return trip_link, itineraries, legs, segments