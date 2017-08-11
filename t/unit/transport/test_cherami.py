### Unit Test for Cherami Transport (pip install cherami-client) ###
### This test requires a environment variable CLAY_CONFIG, which is the path to the config file
### e.g. export CLAY_CONFIG=./t/configs/cherami_test.yaml

from __future__ import absolute_import, unicode_literals

import unittest, time

from case import Mock, MagicMock, skip

from kombu import Connection
from kombu.async import Hub
from kombu.utils.json import loads, dumps
from kombu.transport import cherami


class CheramiMessage():

    def __init__(self, data):
        self.payload = self.CheramiPayload(data)

    class CheramiPayload():

        def __init__(self, data):
            self.data = data


@skip.unless_module('cherami_client')
class test_CheramiChannel(unittest.TestCase):

    def setUp(self):

        self.queue_name = 'unittest'

        self.connection = Connection(transport=cherami.Transport)
        self.channel = self.connection.channel()

        # set up the kombu async event loop object
        self.channel.hub = Hub()

        # set up mock cherami client publisher and consumer
        self.channel._publisher = Mock()
        self.channel._consumer = Mock()


    def tearDown(self):
        # Removes QoS reserved messages so we don't restore msgs on shutdown.
        try:
            qos = self.channel._qos
        except AttributeError:
            pass
        else:
            if qos:
                qos._dirty.clear()
                qos._delivered.clear()

    def test_basic_consume(self):
        # make sure the first consume call registers the loop
        self.channel._loop = Mock()

        self.channel.basic_consume(self.queue_name, True, None, None)
        self.channel._loop.assert_called_once_with(self.queue_name)

    def test_loop(self):
        # make sure the loop call does use hub to register event loop
        self.channel.hub.call_repeatedly = Mock()

        self.channel._loop(self.queue_name)
        self.channel.hub.call_repeatedly.assert_called_once_with(self.channel.default_consume_interval, self.channel._get, self.queue_name)

    def test_put(self):
        message = {'foo': 'bar'}
        self.channel._publisher.publish = Mock()

        self.channel._put(self.queue_name, message)
        self.channel._publisher.publish.assert_called_with(str(0), dumps(message))

        message = {'bar': 'foo'}
        self.channel._put(self.queue_name, message)
        self.channel._publisher.publish.assert_called_with(str(0), dumps(message))

    def test_get(self):
        delivery_token = 123456
        message = CheramiMessage({'foo': 'bar'})
        results = [[delivery_token, message]]
        self.channel._consumer.receive = Mock(return_value=results)
        self.channel._on_message_ready = Mock()

        self.channel._get(self.queue_name)
        self.channel._consumer.receive.assert_called_once_with(num_msgs=self.channel.default_message_batch_size)
        self.channel._on_message_ready.assert_called_once_with(self.queue_name, results)

    def test_on_message_ready(self):
        self.channel._consumer.ack = Mock()
        self.channel._consumer.nack = Mock()

        # test regular message and ack
        delivery_token = 123456
        message = CheramiMessage({'foo': 'bar'})
        results = [[delivery_token, message]]
        self.channel._handle_message = Mock()
        self.channel._on_message_ready(self.queue_name, results)
        self.channel._handle_message.assert_called_with(self.queue_name, {'foo': 'bar'})
        self.channel._consumer.ack.assert_called_once_with(delivery_token)

        # test handle message exception and nack
        delivery_token = 654321
        message = CheramiMessage({'bar': 'foo'})
        results = [[delivery_token, message]]
        self.channel._handle_message = Mock(side_effect=Exception)
        self.channel._on_message_ready(self.queue_name, results)
        self.channel._handle_message.assert_called_with(self.queue_name, {'bar': 'foo'})
        self.channel._consumer.nack.assert_called_with(delivery_token)

        # test bad message format exception and nack
        delivery_token = 000000
        message = {'foo': 'foo'}
        results = [[delivery_token, message]]
        self.channel._on_message_ready(self.queue_name, results)
        self.channel._consumer.nack.assert_called_with(delivery_token)

    def test_handle_message(self):
        # make sure the callback is called
        message = dumps(dict({'foo': 'bar'}))
        self.channel.connection._callbacks[self.queue_name] = Mock()

        self.channel._handle_message(self.queue_name, message)
        self.channel.connection._callbacks[self.queue_name].assert_called_once_with(loads(message))

    def test_close(self):
        self.channel._publisher.close = Mock()
        self.channel._consumer.close = Mock()
        self.channel.close()

        self.channel._publisher.close.assert_called_once()
        self.channel._consumer.close.assert_called_once()
