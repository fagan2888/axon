# stats
from statsd import StatsClient

# vibe
from vibepy.class_postgres import PostgresManager
from vibepy import read_config

# logging
import logging
import graypy

# local
from exchange_callback import ExchangeCallback
from rabbit_consumer import RabbitConsumerProc, RabbitConsumerThread


class AbstractBot(object):

    db = None
    statsd = None

    def __init__(self, bot_id_setting, config, exchange_callbacks, callback_consumer_num=1, use_threading=False):

        self.config = config

        self.bot_id = 'bot-' + self.config.get('bot', bot_id_setting)

        self.configure_logging()

        self.log_config()

        sects = self.config.sections()
        if 'postgres' in sects:
            self.db = self.__build_db_pool__(self.config)

        if 'statsd' in sects:
            self.statsd = self.__build_statsd_client__(self.config)

        if self.statsd is None:
            self.logger.error("Statsd config must be supplied! Section [statsd] missing from config")
            exit()

        if use_threading:
            self.rabbit_klass = RabbitConsumerThread
        else:
            self.rabbit_klass = RabbitConsumerProc

        exchange_callbacks = self.__configure_callbacks__(exchange_callbacks, callback_consumer_num)
        self.consumers = self.__build_consumers__(exchange_callbacks)

    def read_config(self, config_filename, config_filepath):
        self.config = read_config.read_config(ini_filename=config_filename,
                                              ini_path=config_filepath)

    def stop(self):
        self.logger.info("Stopping consumers")
        self.__stop_consumers__()
        exit()

    def start(self):
        self.logger.info("Starting consumers")

        try:
            self.__start_consumers__()

        except (KeyboardInterrupt, SystemExit):
            self.logger.info('Received keyboard interrupt, quitting')
            self.stop()
        except Exception as e:
            # all other errors
            self.logger.info("Unexpected error - {0}".format(e))
            self.stop()
            raise

    def __build_db_pool__(self, config):
        logging.info("Creating PostgresManager")

        max_connections = 10
        if config.has_option('logging', 'POSTGRES_MAX_CONNECTIONS'):
            max_connections = int(config.get('postgres', 'POSTGRES_MAX_CONNECTIONS'))

        return PostgresManager(config, 'postgres', max_connections=max_connections)

    def __build_statsd_client__(self, config):
        statsdhost = config.get('statsd', 'STATSD_HOST')
        statsdport = config.get('statsd', 'STATSD_PORT')
        statsd_namespace = config.get('statsd', 'STATSD_NAMESPACE')
        statsd_namespace += "." + self.bot_id
        self.logger.info("Creating StatsClient with prefix " + statsd_namespace)
        return StatsClient(host=statsdhost, port=statsdport, prefix=statsd_namespace, maxudpsize=512)

    def __build_consumers__(self, exchange_callbacks):
        raise NotImplementedError

    def __start_consumers__(self):
        raise NotImplementedError

    def __stop_consumers__(self):
        raise NotImplementedError

    def configure_logging(self):

        log_format = '%(asctime)s [%(processName)-17.17s] [%(levelname)-5.5s] %(message)s'
        if self.config.has_option('logging', 'LOG_FORMAT'):
            log_format = self.config.get('logging', 'LOG_FORMAT', raw=True)

        log_level = self.config.get('logging', 'LOG_LEVEL')

        # Root logging configuration
        log_formatter = logging.Formatter(log_format)
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)

        # Graylog configuration
        if self.config.has_option('logging', 'GRAYLOG_SERVER'):
            graylog_server = self.config.get('logging', 'GRAYLOG_SERVER')
            graylog_port = int(self.config.get('logging', 'GRAYLOG_PORT'))
            handler = graypy.GELFHandler(graylog_server, graylog_port, facility=self.bot_id)
            root_logger.addHandler(handler)

        self.logger = logging.getLogger(__name__ + "_" + self.bot_id)

    def log_config(self):
        config_str = ""

        for sect in self.config.sections():
            config_str += "[" + sect + "]\n"
            for k, v in dict(self.config.items(sect, raw=True)).iteritems():
                config_str += "{0} : {1}\n".format(k, v)

            config_str += '\n'

        self.logger.info("Config:\n" + config_str)

    def __configure_callbacks__(self, exch_callbacks, num_callbacks):
        if isinstance(exch_callbacks, dict):
            new_exch_callbacks = []
            for exch, callback in exch_callbacks.iteritems():
                new_exch_callbacks.append(ExchangeCallback(exch, callback, num_callbacks))
            return new_exch_callbacks

        return exch_callbacks