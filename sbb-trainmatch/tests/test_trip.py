import unittest
from mock import MagicMock, Mock, patch
import pandas as pd

from event.sbbrequest.trip import Trip
from tests.data_structures import TRIP_SERIES

class TripTest(unittest.TestCase):
    BATCH_ID = 1
    CONFIG = {'rabbitmq': '3'}

    def setUp(self):
        self.config = Mock()

        with patch("event.sbbrequest.trip.SBBPublisherBot") as pub_bot:
            pub_bot.return_value = Mock()
            self.trip = Trip(pd.Series(TRIP_SERIES), self.BATCH_ID, self.CONFIG)

    def test_GenParamSeg(self):
        params = self.trip.gen_param_seg()

        self.assertTrue(params.has_key('from_lon'))
