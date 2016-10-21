import time
import logging


class ConsumerCallback(object):

    logger = logging.getLogger(__name__)

    def __init__(self, consumer_id, exchange, callback_func):
        self.consumer_id = consumer_id
        self.exchange = exchange
        self.callback_func = callback_func
        self.function_calls = 0
        self.total_execution_time = 0

    def timed_callback_execution(self, func_arg):
        self.function_calls += 1
        start = time.time()
        func_ret = self.callback_func(func_arg)
        exec_time = int((time.time() - start) * 1000)
        self.total_execution_time += exec_time

        self.logger.debug("Consumer {0} Callback execution time: {1}ms".format(self.consumer_id, exec_time))

        # if we have processed 100 callbacks, log out the average execution time at INFO then reset the total
        if self.function_calls % 2 == 0:
            average_execution_time = self.total_execution_time / 100
            self.logger.info("Consumer {0} Avg callback execution time (last 100): {1}ms".format(self.consumer_id,
                                                                                                 average_execution_time))
            self.total_execution_time = 0

        return func_ret
