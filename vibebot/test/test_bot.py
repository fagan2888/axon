
import os

from vibebot import EventBot

from vibebot.exchange_callback import ExchangeCallback

# Reference impl of the EventBot, Test Bot. Omitted from the vibebot archive
class TestBot(EventBot):

    def __init__(self):

        self.read_config('settings-test.ini', os.path.dirname(__file__))

        exchange_callback_one = ExchangeCallback(self.config.get('rabbit', 'RABBIT_INBOUND_TEST_EXCHANGE_ONE'),
                                                 self.callback_one,
                                                 int(self.config.get('rabbit', 'CALLBACK_ONE_CONSUMERS')))

        exchange_callback_two = ExchangeCallback(self.config.get('rabbit', 'RABBIT_INBOUND_TEST_EXCHANGE_TWO'),
                                                 self.callback_two,
                                                 int(self.config.get('rabbit', 'CALLBACK_TWO_CONSUMERS')))

        # exchange_callback_three = ExchangeCallback(self.config.get('rabbit', 'RABBIT_INBOUND_TEST_EXCHANGE_THREE'),
        #                                            self.callback_three)

        exchange_callbacks = [
            exchange_callback_one, exchange_callback_two
        ]

        super(TestBot, self).__init__('TEST_BOT_ID', self.config, exchange_callbacks, use_threading=False)

        self.PUBLISHING_QUEUE = self.config.get('rabbit', 'RABBIT_OUTBOUND_TEST_QUEUE')

        self.logger.info("TestBot created ready for testing")

    def callback_one(self, json_body):
        if 'force_error' in json_body: # will force the message to go onto the error queue
            raise Exception
        self.logger.info("Callback one in test bot received message: - " + str(json_body))
        return []

    def callback_two(self, json_body):
        self.logger.info("Callback in test bot received message: - " + str(json_body))
        return []

    def callback_three(self, json_body):
        self.logger.info("Callback in test bot received message: - " + str(json_body))

        if self.config.has_section('postgres'):
            # test db if configured - this varibale (a db connection pool) is provided by the super class, AbstractBot
            self.db.execute("select 1")

        return []



