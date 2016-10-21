import logging
import json
import time

import pika

from vibebot.rabbit_consumer import RabbitConsumer

logger = logging.getLogger(__name__)


class RabbitPublisherConsumer(RabbitConsumer):

    def on_message(self, unused_channel, basic_deliver, properties, body):
        logger.info(
            u" [{}] received message #{} from exchange {}: {}".format(self.bot_id,
                                                                     basic_deliver.delivery_tag, self.exchange,
                                                                     body.decode('utf-8')))
        # Ack the message before processing to tell rabbit we got it.
        # TODO before sending ack we should persist the message in a local queue to avoid the possibility of losing it
        self.acknowledge_message(basic_deliver.delivery_tag)

        try:
            json_body = json.loads(body)
            response_messages = self.callback_func(json_body)

            logger.info(" [{}] Sending {} response messages".format(self.bot_id, len(response_messages)))

            for message in response_messages:
                self._channel.basic_publish(exchange=message['exchange'], routing_key=message['queue'],
                                            body=message['body'])
                logger.info(" [{}] published message {}".format(self.bot_id, message))

        except ValueError:
            logger.exception(
                " [{}] Invalid JSON received from exchange {}, dropping message".format(self.bot_id, self.exchange))

        except:
            # todo: better exception handling this is not good practice
            logger.exception(
                " [{}] Unexpected error, received from exchange {}, dropping message".format(self.bot_id,
                                                                                             self.exchange))


class PubSubBot(object):
    def __init__(self, pub_creds, sub_creds):
        pubc = pika.PlainCredentials(username=pub_creds['rabbit_user'], password=pub_creds['rabbit_pw'])
        self.publisher = pika.BlockingConnection(pika.ConnectionParameters(host=pub_creds['rabbit_host'],
                                                                           port=pub_creds['rabbit_port'],
                                                                           credentials=pubc))

        # setup publishing
        self.pub_channel = self.publisher.channel()
        self.publisher_started = True

        # deal with timeout for receiving responses
        self.processing_begun = False
        self.max_time = 600
        self.start_time = 0

        # setup consumer
        self.consumer = RabbitPublisherConsumer(sub_creds['queue'], sub_creds['exchange'], sub_creds['callback_func'], sub_creds['rabbit_user'],
                                       sub_creds['rabbit_pw'], sub_creds['rabbit_host'], sub_creds['rabbit_port'])

    def start(self):
        # this only sets up the consumer - the publisher needs to be called manually (typically before this starts)
        try:
            self.consumer.start()
            while self.consumer.isAlive():
                # let's check how much time has elapsed
                if self.processing_begun:
                    time_elapsed = time.time() - self.start_time
                    if time_elapsed > self.max_time:
                        # timeout reached, shut it down
                        self.consumer.join(1)
                        self.stop()
                self.consumer.join(1)

        except (KeyboardInterrupt, SystemExit):
            self.consumer.stop()

    def stop(self):
        if self.publisher_started:
            # in case we didn't shut down the publisher before hand
            self.publisher.close()

        # unbind the queue
        self.delete_channel()

        # stop the mq connection
        self.consumer.stop()

    def stop_publisher(self):
        self.publisher_started = False
        self.publisher.close()

    def delete_channel(self):
        logger.info("unbinding queue %s" % self.consumer.queue_name)
        self.consumer._channel.queue_unbind(queue=self.consumer.queue_name, exchange=self.consumer.exchange, routing_key='')


class PublisherBot(object):
    def __init__(self, pub_creds):
        pubc = pika.PlainCredentials(username=pub_creds['rabbit_user'], password=pub_creds['rabbit_pw'])
        self.publisher = pika.BlockingConnection(pika.ConnectionParameters(host=pub_creds['rabbit_host'],
                                                                           port=pub_creds['rabbit_port'],
                                                                           credentials=pubc))
        # setup publishing
        self.pub_channel = self.publisher.channel()

    def stop_publisher(self):
        self.publisher.close()


