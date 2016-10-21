import logging
import os
import json
import uuid
from datetime import datetime

from shapely.geometry.point import Point
from shapely.wkt import loads

import pandas as pd
import numpy as np
from geopy.distance import vincenty

# redis
from redis_client import RedisClient
import pickle

from vibebot import EventBot
from vibepy.class_postgres import PostgresManager
from vibepy.read_config import read_config

from batch import Batch
from traineval.output_to_postgres import update_postgres

from sbbrequest import sbb_response

CONFIG = read_config(ini_filename='application.ini', ini_path=os.path.dirname(__file__))
DB = PostgresManager(CONFIG, 'database')

logger = logging.getLogger(__name__)


class ScheduleMatchingBot(EventBot):

    def __init__(self):

        queues_callbacks = {
            CONFIG.get('rabbit', 'rabbit_mot_exchange'): self.callback_new_mot,
            "spf_response_exchange": self.callback_process_mot
        }

        # load geometries initially to be used for entire session
        self.geoms = self.read_geo_valid()

        # Queue on the exchange the bot are reading from
        # %%RABBIT_MOT_EXCHANGE%%-bot-%%SCHEDULE_MATCHING_BOT_ID%%

        super(ScheduleMatchingBot, self).__init__('sched_matching', CONFIG, queues_callbacks)
        self.redis_client = RedisClient(CONFIG.get('redis','redis_host'), CONFIG.get('redis','redis_port'))

        logger.info("Event bot created")


    def callback_new_mot(self, json_body):

        logging.debug("[received] new MoT segments %r" % json_body)
        # Extract the batch ID and the list mo MoT segments froim the json!

        try:
            batch_id = str(uuid.uuid4())

            if not json_body['mot_segments']:
                logging.debug('mot_segments is empty. Skipping schedule matching.')
                return []

            list_mot_id, loc_bounds = self.geo_valid(json_body['mot_segments'])
            if not list_mot_id:
                logging.debug('no mot id. Skipping schedule matching.')
                return []

        except Exception as e:
            logging.error(e)
            err = 'Unable to parse JSON into a batch id and list of mot ids'
            logging.error(err)
            # we are going to write the error to a separate queue
            return []

        b = Batch(batch_id, list_mot_id, loc_bounds, CONFIG, DB)

        b.init_trips()
        # we have to clear out the DB attr since it can't be pickled
        b.DB = None
        # now we are doing this in redis
        self.redis_client.upload_to_redis(batch_id, {'status': 0, 'batch': pickle.dumps(b)})
        # send reqs
        b.send_trip_requests()

        # Empty list when not sending any message out otherwise rabbit consumer doesn't like it
        return []

    def callback_process_mot(self, json_body):
        logging.debug("[received] new SBB response")
        status = 1
        try:
            # first let's put this in redis
            self.redis_client.upload_to_redis(json_body.get('uuid'), json_body.get('xml'))
            batch_id, trip_id, max_res, leave_at = json_body.get('uuid').split("_")
            if not batch_id:
                return []

            # get obj
            # now in redis
            b_binary = self.redis_client.get_hm_obj(batch_id, 'batch')[0]
            if not b_binary:
                logging.warning('batch not found ({bi})'.format(bi=batch_id))
                return []
            else:
                b = pickle.loads(b_binary)
                # now we can lock up this batch for processing
                # we need to update DB
                b.DB = DB

                # ok, let's see which trip this is
                trip = b.trip_objs[trip_id]
                if trip.request_params[(max_res, leave_at)] == 0:
                    # this request hasn't been processed yet
                    resp = sbb_response.SBBResponse(json_body['xml'].encode('utf-8'))
                    good_to_go = resp.check_if_error()
                    if good_to_go > 0:
                        # we are either going to retry or skip
                        if good_to_go == 2:
                            return []  # skipping
                        else:
                            # we will republish the request
                            trip.republish_req([(max_res, leave_at)])
                            return []

                    trip.build_single_itinerary(resp, max_res, leave_at)
                    if trip.requests_processed == len(trip.request_params):
                        # we've processed everything
                        b.trips_processed += 1
                        trip.complete_processing()

                    if b.trips_processed == len(b.trip_objs):
                        # we are done processing
                        logging.debug("batch %s ready for processing" % b.batch_id)
                        b.process_trips()
                        status = 2

                    # we need to write the updated obj back in redis
                    b.DB = None  # bye, you can't be pickled
                    self.redis_client.upload_to_redis(batch_id, {'status' : status, 'batch' : pickle.dumps(b)})

        except Exception as e:
            logging.error(e)
            err = 'Unable to process batch'
            logging.error(err)
            return []

        del b
        return []

    def read_geo_valid(self):
        """
        :param CONFIG: The parsed config file
        :return: A list of geometries (polygons) defining out geo valid regions
        """
        path = os.path.dirname(os.path.realpath(__file__))
        filenames = CONFIG.get('geovalidity', 'VALID_REGION_WKT').split(',')
        geoms = []
        for filename in filenames:
            filepath = path + '/filters/wkt/' + filename + '.wkt'
            try:
                f = open(filepath)
                geoms.append(loads(f.read()))
            except:
                logging.critical('Could not read shapefile {filepath}'.format(filepath=filepath))
                raise IOError
        return geoms

    def geo_valid(self, json_input):
        """
        In order to make a guess at MoT we have to have OSM data. geoms is a list of geometries (closed polygons)
        that define the regions of validity of this model. This function returns a list of visit id's that are valid
        TODO: If we don't specify a lat and lon, go look this up.
        :param visits: A list of dictionaries containing a lat, a lon and a visit id
        :param CONFIG: The parsed config file
        :return: Will output a list of visits that are within the regions of validity
        """

        # geoms = read_geo_valid(CONFIG)
        p = pd.DataFrame(json_input)

        p = p[p['mot'] == 'train']
        if p.empty:
            logging.debug('No train journey within json')
            return [], []

        # Is within region of geovalidity
        is_within = p.apply(lambda x: np.all([geom.contains(Point(x['start_lon'], x['start_lat'])) and
                                              geom.contains(Point(x['end_lon'], x['end_lat'])) for geom in self.geoms]),
                            axis=1).values
        # is greater than the min distance
        is_gt_min_dist = p.apply(lambda x: vincenty((x['start_lat'], x['start_lon']),(x['end_lat'], x['end_lon'])).meters
                                           > CONFIG.getint('geovalidity', 'MIN_DIST_BTW_PTS'),
                            axis=1).values
        # If trips need to be discarded, log in in the failed trips table
        if any(~is_within) or any(~is_gt_min_dist):
            p['failure_cause'] = ''
            p['datetime_created'] = datetime.utcnow()

            error_msg = 'Not within region of geo-validity ({}). '
            p.loc[~is_within, 'failure_cause'] += error_msg.format(CONFIG.get('geovalidity', 'VALID_REGION_WKT'))

            error_msg = 'Start and end point of MoT segment too close (within {} meters of each-other). '
            p.loc[~is_gt_min_dist, 'failure_cause'] += error_msg.format(CONFIG.getint('geovalidity', 'MIN_DIST_BTW_PTS'))

            col_list = ['mot_segment_id', 'failure_cause', 'datetime_created', 'bound_from_id', 'bound_to_id']
            failed_trips = p.loc[p['failure_cause'] != '', col_list]
            update_postgres(failed_trips, 'train_trips_failed', DB)

        mask = np.logical_and(is_within, is_gt_min_dist)
        loc_bounds = p.loc[mask, ['mot_segment_id', 'bound_from_id', 'bound_to_id']]
        return p.loc[mask, 'mot_segment_id'].tolist(), loc_bounds
