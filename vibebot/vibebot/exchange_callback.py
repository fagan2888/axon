class ExchangeCallback(object):
    def __init__(self, exchange, callback_func, consumer_count=1):
        self.exchange = exchange
        self.callback_func = callback_func
        self.consumer_count = consumer_count




