########################################################
# Copyright 2019-2021 program was created VMware, Inc. #
# SPDX-License-Identifier: Apache-2.0                  #
########################################################

import time

import pulsar
from pulsar import _pulsar
from fate_arch.common import log

LOGGER = log.getLogger()
CHANNEL_TYPE_PRODUCER = 'producer'
CHANNEL_TYPE_CONSUMER = 'consumer'
DEFAULT_TENANT = 'fl-tenant'
DEFAULT_CLUSTER = 'standalone'
TOPIC_PREFIX = DEFAULT_TENANT + '/{}/{}'
UNIQUE_PRODUCER_NAME = 'unique_producer'
UNIQUE_CONSUMER_NAME = 'unique_consumer'
DEFAULT_SUBSCRIPTION_NAME = 'unique'


def connection_retry(func):
    """retry connection
    """

    def wrapper(self, *args, **kwargs):
        """wrapper
        """
        res = None
        for ntry in range(60):
            try:
                res = func(self, *args, **kwargs)
                break
            except Exception as e:
                LOGGER.error("function %s error" %
                             func.__name__, exc_info=True)
                time.sleep(0.1)
        return res
    return wrapper

 # A channel cloud only be able to send or receive message.


class MQChannel(object):
    # TODO add credential to secure pulsar cluster
    def __init__(self, host, port, pulsar_namespace, pulsar_send_topic, pulsar_receive_topic, party_id, role, credential=None, extra_args: dict = None):
        # "host:port" is used to connect the pulsar broker
        self._host = host
        self._port = port
        self._namespace = pulsar_namespace
        self._send_topic = pulsar_send_topic
        self._receive_topic = pulsar_receive_topic
        self._credential = credential
        self._party_id = party_id
        self._role = role
        self._extra_args = extra_args

        # "_conn" is a "client" object that handles pulsar connection
        self._conn = None
        # "_channel" is the subscriptor for the topic
        self._producer_send = None
        self._consumer_send = None

        self._producer_receive = None
        self._consumer_receive = None

        self._producer_config = {}
        if extra_args.get('producer') is not None:
            self._producer_config.update(extra_args['producer'])

        self._consumer_config = {}
        if extra_args.get('consumer') is not None:
            self._consumer_config.update(extra_args['consumer'])

    @property
    def party_id(self):
        return self._party_id

    @connection_retry
    def basic_publish(self, body, properties):
        self._get_channel()
        LOGGER.debug(f"send queue: {self._send_topic}")
        LOGGER.debug(f"send data: {body}")
        self._producer_send.send(content=body, properties=properties)

    @connection_retry
    def consume(self):
        self._get_channel()
        # since consumer and topic are one to one corresponding, maybe it is ok to use unique subscription name?
        LOGGER.debug('receive topic: {}'.format(
            self._consumer_receive.topic()))
        message = self._consumer_receive.receive()
        return message

    @connection_retry
    def basic_ack(self, message):
        self._get_channel()
        return self._consumer_receive.acknowledge(message)

    @connection_retry
    def cancel(self):
        self._get_channel()
        return self._consumer_receive.close()

    @connection_retry
    def _get_channel(self):
        if self._check_alive():
            return
        else:
            LOGGER.debug(
                "trigger clean, cleaning"
            )
            self._clear()

        if not self._conn:
            self._conn = pulsar.Client(
                'pulsar://{}:{}'.format(self._host, self._port))

        # TODO: it is little bit dangerous to pass _extra_args here ;)
        # TODO: find a batter way to avoid pairs
            self._producer_send = self._conn.create_producer(TOPIC_PREFIX.format(self._namespace, self._send_topic),
                                                             producer_name=UNIQUE_PRODUCER_NAME,
                                                             **self._producer_config)

            '''
            self._consumer_send = self._conn.subscribe(TOPIC_PREFIX.format(self._namespace, self._send_topic),
                                                       subscription_name=DEFAULT_SUBSCRIPTION_NAME,
                                                       consumer_name=UNIQUE_CONSUMER_NAME,
                                                       **self._consumer_config)

            self._producer_receive = self._conn.create_producer(TOPIC_PREFIX.format(self._namespace, self._receive_topic),
                                                                producer_name=UNIQUE_PRODUCER_NAME,
                                                                **self._producer_config)
            '''

            self._consumer_receive = self._conn.subscribe(TOPIC_PREFIX.format(self._namespace, self._receive_topic),
                                                          subscription_name=DEFAULT_SUBSCRIPTION_NAME,
                                                          consumer_name=UNIQUE_CONSUMER_NAME,
                                                          initial_position=_pulsar.InitialPosition.Earliest,
                                                          **self._consumer_config)

    def _clear(self):
        try:
            if self._conn is not None:
                self._conn.close()
            self._conn = None
            self._producer_send = None
            self._consumer_receive = None

        except Exception as e:
            LOGGER.exception(e)
            self._conn = None
            self._producer_send = None
            self._consumer_receive = None

    def _check_alive(self):
        # a tricky way to check alive ;)
        try:
            self._conn.get_topic_partitions('test-alive')
            self._consumer_receive.consume()
            return True
        except Exception:
            return False
