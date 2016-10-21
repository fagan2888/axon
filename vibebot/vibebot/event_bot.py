import Queue

import time

from rabbit_consumer import RabbitConsumerProc, RabbitConsumerThread
from abstract_bot import AbstractBot
from vibebot.ConsumerCallback import ConsumerCallback


class EventBot(AbstractBot):
    def __build_consumers__(self, exchange_callbacks):

        self.logger.info("Event bot created v0.0.11")

        if len(exchange_callbacks) == 0:
            self.logger.error("No callbacks declared, exiting EventBot")
            exit()

        consumers = {}
        consumer_id_ctr = 0
        self.consumers_callbacks = {}
        self.internal_error_queue = Queue.Queue()

        self.rabbit_user = self.config.get('rabbit', 'RABBIT_USER')
        self.rabbit_pw = self.config.get('rabbit', 'RABBIT_PW')
        self.rabbit_host = self.config.get('rabbit', 'RABBIT_HOST')
        self.rabbit_port = int(self.config.get('rabbit', 'RABBIT_PORT'))
        self.stopping = False

        for exchange_callback in exchange_callbacks:
            for consumerCount in range(0, exchange_callback.consumer_count):
                self.logger.info("Creating exchange callback for exchange: %s with consumer id: %s", exchange_callback.exchange, consumer_id_ctr)
                consumer_callback = ConsumerCallback(consumer_id_ctr, exchange_callback.exchange, exchange_callback.callback_func)
                thread = self.rabbit_klass(self.bot_id, exchange_callback.exchange, self.callback_wrapper(consumer_callback), self.rabbit_user, self.rabbit_pw,
                                      self.rabbit_host, self.rabbit_port, consumer_id_ctr, self.internal_error_queue, self.statsd)
                consumers[consumer_id_ctr] = thread
                self.consumers_callbacks[consumer_id_ctr] = consumer_callback
                consumer_id_ctr += 1

        self.logger.info("Event bot created!")
        return consumers

    def __start_consumers__(self):

        for thread in self.consumers.values():
            thread.start()

        while not self.stopping:
            time.sleep(2)
            while not self.internal_error_queue.empty():
                consumer_id = self.internal_error_queue.get()
                self.logger.warn("Internal error detected restarting consumer:" + str(consumer_id))

                thread = self.consumers[consumer_id]
                callback = self.consumers_callbacks[consumer_id]
                # Exception thrown so this threadess should be dead - call join
                thread.join()

                new_thread = self.rabbit_klass(self.bot_id, callback.exchange, self.callback_wrapper(callback), self.rabbit_user,
                                          self.rabbit_pw, self.rabbit_host, self.rabbit_port, consumer_id, self.internal_error_queue, self.statsd)

                self.consumers[consumer_id] = new_thread

                self.statsd.incr(callback.exchange + "." + 'callback.restart')
                new_thread.start()

    def __stop_consumers__(self):
        self.stopping = True
        for thread in self.consumers.values():
            # thread.stop() # gonna need this for threads
            thread.total_stop()
            # if thread.is_alive():
            #     self.logger("threadess '{}' not finished after 10secs, forcibly terminating", thread.consumer_id)
            #     thread.terminate()

    def callback_wrapper(self, consumer_callback):
        def _inner(json):
            ret = consumer_callback.timed_callback_execution(json)
            return ret
        return _inner