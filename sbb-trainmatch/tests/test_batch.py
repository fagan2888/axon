import unittest
from mock import MagicMock, Mock, patch
from event.getstops import getstops
import pytest
import pandas as pd

from event.batch import Batch
from tests.data_structures import TRIPS_DF, TRIP_LINK_DF, ITINERARY_DF, LEGS_DF, SEGMENTS_DF

class BatchTest(unittest.TestCase):
    BATCH_ID = 1
    LIST_MOT_ID = '86a42e1a-fc08-459f-82e1-2b113d4be97b'
    TRIPS_DF = pd.DataFrame(TRIPS_DF)


    def setUp(self):
        self.db = Mock()
        self.config = Mock()
        with patch("event.getstops.getstops.get_stops") as gs:
            gs.return_value = self.TRIPS_DF
            self.batch = Batch(self.BATCH_ID, self.LIST_MOT_ID, self.db, self.config)

    # @patch("event.getstops.getstops.get_stops", return_value=["test"])
    # def test_CreateBatch(self, get_stops):
    #     batch =
    #
    #     self.assertEqual(batch.trips, ["test"])

    @patch("event.batch.Trip", Mock())
    def test_InitTrips(self):
        # batch = Batch(self.BATCH_ID, self.LIST_MOT_ID, self.db, self.config)
        self.batch.init_trips()

        self.assertEqual(len(self.batch.trip_objs), 1)

        # with patch('event.sbbrequest.trip.Trip') as trip_class:
        #     batch = Batch(self.BATCH_ID, self.LIST_MOT_ID, self.db, self.config)
        #     trip = MagicMock()
        #     trip_class.return_value = trip
        #     batch.init_trips()

    @patch("event.batch.Trip.publish_reqs")
    def test_SendTripRequests(self, pub_req):
        self.batch.send_trip_requests()

    def test_BuildTripDfs(self):
        t = Mock()
        self.batch.trip_objs = {"blah": t}
        t.trip_link_df = pd.DataFrame(TRIP_LINK_DF)
        t.itinerary_df = pd.DataFrame(ITINERARY_DF)
        t.legs_df = pd.DataFrame(LEGS_DF)
        t.segments_df = pd.DataFrame(SEGMENTS_DF)

        tl, it, lg, seg = self.batch.build_trip_dfs()

        self.assertEqual(tl.shape, t.trip_link_df.shape)


    @patch("event.batch.calc_distances", return_value=[1, 2])
    @patch("event.batch.fpga", return_value=[1, 2])
    @patch("event.batch.get_best_itinerary", return_value=[1, 2])
    @patch("event.batch.write_metrics")
    @patch("event.batch.save_output")
    @patch("event.batch.save_failed_trips")
    def test_ProcessTrips(self, save_failed_trips_mock, save_output_mock, write_metrics_mock, get_best_itinerary_mock, fpga_mock, calc_distances_mock):

        t = Mock()
        self.batch.trip_objs = {"blah": t}
        t.trip_link_df = pd.DataFrame(TRIP_LINK_DF)
        t.itinerary_df = pd.DataFrame(ITINERARY_DF)
        t.legs_df = pd.DataFrame(LEGS_DF)
        t.segments_df = pd.DataFrame(SEGMENTS_DF)

        self.batch.process_trips()

