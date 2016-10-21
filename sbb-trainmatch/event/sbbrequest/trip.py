# Core python
import logging
import os
import pika
import uuid
import xml.etree.cElementTree as ET
import datetime
from datetime import timedelta
import ast

import json

# sci stack
import pandas as pd
import numpy as np

# Vibe
from rabbit_publisher_consumer import PublisherBot

# SBB
import itinerary
import init_data_struct as ids
import xml_path
import remove_itineraries as ri

TRIP_CACHE = dict()
LEG_SUB_TYPES = ['S','IR','R','RE','EC','RJ','ICE','IC','ICN','VAE','TGV']

def roundTime(dt=None, dateDelta=datetime.timedelta(minutes=1), to='average'):

    """Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
            Stijn Nevens 2014 - Changed to use only datetime objects as variables
    """
    roundTo = dateDelta.total_seconds()

    if dt == None : dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds
    if to == 'up':
        rounding = (seconds + roundTo) // roundTo * roundTo
    elif to == 'down':
        rounding = seconds // roundTo * roundTo
    else:
        # // is a floor division, not a comment on following line:
        rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def check_trip_cache(params, max_res, leave_at):
    ret = TRIP_CACHE.get((params['from_lat'], params['from_lon'], params['to_lat'], params['to_lon'], params['rounded_timestamp'],
                           max_res, leave_at))
    # periodic clearing - we can probably figure out a better way to do this
    if len(TRIP_CACHE) > 10000:
        TRIP_CACHE.clear()
    return ret

class SBBPublisherBot(PublisherBot):
    def publish(self, trip, loop_through, exchange, routing_key):
        xml_str_fname = os.path.dirname(os.path.realpath(__file__)) + '/xml/sbb_api.xml'
        request_exchange, response_exchange = exchange[0], exchange[1]
        for l in loop_through:
            params = trip.gen_param_seg(MaxResultNumber=int(l[0]), leave_at=ast.literal_eval(l[1]))
            req_xml = trip.gen_query_xml_str(params, xml_str_fname)
            max_res = l[0]
            leave_at = l[1]
            msg = {"uuid": trip.batch_id + "_" + trip.trip_id + "_" + max_res + "_" + leave_at, "xml": req_xml}
            trip.params[(max_res, leave_at)] = params
            properties = pika.BasicProperties(app_id='example-publisher',
                                              content_type='application/json',
                                              headers=msg)
            resp = check_trip_cache(params, max_res, leave_at)
            if resp:
                logging.debug("Duplicate trip, sending XML response")
                json_resp = {"uuid": trip.batch_id + "_" + trip.trip_id + "_" + max_res + "_" + leave_at, "xml": resp}
                self.pub_channel.basic_publish(response_exchange, routing_key, json.dumps(json_resp, ensure_ascii=True).encode('utf8'),
                                           properties)
            else:
                # channel.basic_publish('spf_request_exchange', 'spf_response_queue', json.dumps(msg, ensure_ascii=True), properties)
                self.pub_channel.basic_publish(request_exchange, routing_key, json.dumps(msg, ensure_ascii=False).encode('utf8'),
                                               properties)


class Trip(object):

    def __init__(self, trip, batch_id, CONFIG):
        self.config = CONFIG

        self.trip = trip

        self.trip_id = str(uuid.uuid4())

        self.batch_id = batch_id

        self.trip_link_df, self.itinerary_df, self.legs_df, self.segments_df = ids.initialize_all_empty_df()

        self.itineraries = []

        # self.request_params = [(6, True), (6, False), (-6, True), (-6, False)]
        # key here - 0 = Started, 1 = in process, 2 = finished
        self.request_params = {("6", "True") : 0, ("6", "False") : 0, ("-6", "True") : 0, ("-6", "False") : 0}

        self.requests_processed = 0

        self.params = dict()

        self.pub_creds = {'rabbit_user': self.config.get('rabbit', 'rabbit_user'),
                         'rabbit_pw': self.config.get('rabbit', 'rabbit_pw'),
                         'rabbit_host': self.config.get('rabbit', 'rabbit_host'),
                         'rabbit_port': int(self.config.get('rabbit', 'rabbit_port'))}


    def publish(self, publish_params):
        # this is our little sub-pub bot that handles publishing requests and listening for responses
        bot = SBBPublisherBot(self.pub_creds)

        bot.publish(self, publish_params, ["spf_request_exchange", "spf_response_exchange"], '')

        # stop the publisher
        bot.stop_publisher()
        # remove
        del bot

    def publish_reqs(self):
        logging.debug("Publishing requests for trip %s" % self.trip_id)

        self.publish(self.request_params.keys())

    def republish_req(self, publish_param):
        logging.debug("Republishing request for trip %s" % self.trip_id)

        self.publish(publish_param)

    def complete_processing(self):
        # concat stuff
        self.concat_trip_dfs()
        self.concat_legs_dfs()
        self.concat_seg_dfs()

        if not self.trip_link_df.empty:
            self.trip_link_df['vid'] = self.trip['vid']
            self.trip_link_df['mot_segment_id'] = self.trip['mot_segment_id']

            # Not using vid / mot_segment_id as indexes since they're identical for all...
            # Hierarchical indexes need to be sorted for faster operation (uses np.searchsorted )
            self.trip_link_df.sort_values(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)

        self.trip_link_df.set_index(['itinerary_id', 'leg_id', 'segment_id', ], inplace=True)


    def gen_param_seg(self, MaxResultNumber=3, leave_at=True, api_version='v2'):
        # Some parameters need a bit of reformatting
        params = {
            'api_version': api_version,  # check, there might some other dependencies inside the XML...
            'MaxResultNumber': MaxResultNumber,
            'from_lat': int(round(self.trip['lat_start'] * 10 ** 6)),  # int, lat/lon * 1e6
            'from_lon': int(round(self.trip['lon_start'] * 10 ** 6)),
            'to_lat': int(round(self.trip['lat_end'] * 10 ** 6)),
            'to_lon': int(round(self.trip['lon_end'] * 10 ** 6)),

            # These times are used by python but not for the XML query so no reformat
            'trip_time_start': self.trip['trip_time_start'],  # previously starttriptime
            'trip_time_end': self.trip['trip_time_end']  # previously endtriptime
        }

        if MaxResultNumber > 0:
            rounding = "down"
        else:
            rounding = "up"

        if leave_at:  # calculate trip to start at this time
            params['timestamp'] = self.trip['time_start'].strftime(
                "%Y-%m-%dT%H:%M:%S")  # timestamp format: 2015-08-04T14:00:00
            params['DateTimeType'] = 'ABFAHRT'
            params['rounded_timestamp'] = roundTime(self.trip['time_start'], to=rounding)
        else:  # calculate trip to arrive by this time
            params['timestamp'] = self.trip['time_end'].strftime(
                "%Y-%m-%dT%H:%M:%S")  # timestamp format: 2015-08-04T14:00:00
            params['DateTimeType'] = 'ANKUNFT'
            params['rounded_timestamp'] = roundTime(self.trip['time_end'], to=rounding)

        params['mot_segment_id'] = self.trip['mot_segment_id']

        return params

    def gen_query_xml_str(self, params, xml_str_fname):
        """
        XML query that calls the SBB API
        params are the lat/lon/timestamp/MaxResultNumber which specify the query
        """

        if os.path.isfile(xml_str_fname):
            with open(xml_str_fname, "r") as myfile:
                request_str = myfile.read()
            return request_str.format(**params)

        else:
            logging.error('XML template file not found at: {p}'.format(p=xml_str_fname))
            raise IOError


    def build_single_itinerary(self, response, max_res, leave_at):
        """
            Takes in the XML response and turns it into an etree. Then builds tables containing itinerary/leg/segment including
            the node in the tree where they reside.
            The build_ functions then populate all fields of the dataframes with the data associated with that node in the
            etree. All the paths for the various fields are stored as functions in the xml_path.py file.
            Unique IDs are associated to each item (i/l/s) using a uuid4() generator.

            :param response: XML response content from the SBB API call

        """
        # root = ET.fromstring(response.response.content)
        # we are processing now
        self.request_params[(max_res, leave_at)] = 1
        params = self.params.get((max_res, leave_at))
        if params:
            TRIP_CACHE[(params['from_lat'], params['from_lon'], params['to_lat'], params['to_lon'], params['rounded_timestamp'], max_res, leave_at)] = response.response
        root = ET.fromstring(response.response)

        # Extracts the nodes corresponding to itineraries from the tree
        itinerary_nodes = response.get_itinerary_nodes(root)

        # Removes itineraries that have been previously added to this trip
        itinerary_nodes = self.skip_duplicates_itineraries(itinerary_nodes, self.itinerary_df['context_reconstruction'].values, response)
        # itinerary_nodes = ri.skip_duplicates_itineraries(itinerary_nodes,
        #                                                  self.itinerary_df['context_reconstruction'].values)
        # Remove itineraries that overlap with previous/next visit by more than (buffer), a quantity found in CONFIG
        itinerary_nodes = self.skip_visit_overlap_itineraries(self.trip, itinerary_nodes, response)

        # remove bad nodes
        itinerary_nodes = self.remove_unneeded_nodes(itinerary_nodes, response)
        self.requests_processed += 1
        # Only add a new itinerary if there are any nodes
        if len(itinerary_nodes) != 0:
            new_itinerary = itinerary.Itinerary(itinerary_nodes, self.config, response)
            self.itineraries.append(new_itinerary)

            # concat the itinerary df because we need it for future itineraries
            self.itinerary_df = pd.concat([self.itinerary_df, new_itinerary.itinerary_df])

        # we are done processing
        self.request_params[(max_res, leave_at)] = 2



    def concat_trip_dfs(self):
        self.trip_link_df = pd.concat([self.trip_link_df] + [x.trip_link_df for x in self.itineraries], ignore_index=True)

    def concat_legs_dfs(self):
        self.legs_df = pd.concat([self.legs_df] + [x.legs_df for x in self.itineraries])

    def concat_seg_dfs(self):
        self.segments_df = pd.concat([self.segments_df] + [x.segments_df for x in self.itineraries])

    def skip_duplicates_itineraries(self, itinerary_nodes, previous_itineraries_cr, response):

        # Do not recalculate itineraries already included in that trip
        itinerary_nodes = [node for node in itinerary_nodes
                           if not np.in1d(response.get_itin_context_reconstruction(node), previous_itineraries_cr)]

        return itinerary_nodes

    def skip_visit_overlap_itineraries(self, trip, itinerary_nodes, response):

        time_buffer = timedelta(minutes=int(self.config.get('params', 'VISIT_TIME_OVERLAP_BUFFER')))
        min_time = trip['trip_time_start'] - time_buffer
        max_time = trip['trip_time_end'] + time_buffer

        itinerary_nodes = [node for node in itinerary_nodes
                           if (response.get_itin_start_datetime(node) > min_time) and
                           (response.get_itin_end_datetime(node) < max_time)]

        return itinerary_nodes

    def remove_unneeded_nodes(self, itinerary_nodes, response):
        itinerary_nodes = [node for node in itinerary_nodes if self.check_legs(node, response)]

        return itinerary_nodes

    def check_legs(self, node, response):
        for leg in response.get_leg_nodes(node):
            if response.get_leg_type(leg) == "FUSSWEG" or response.get_leg_route_category(leg) in LEG_SUB_TYPES or\
                            (response.get_leg_time_end(leg) - response.get_leg_time_start(leg)) <= timedelta(minutes=5):
                continue
            else:
                return False
        return True





