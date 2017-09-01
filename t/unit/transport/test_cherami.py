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

        # set up async loop object
        self.channel.hub = Mock()

        # set up mock cherami client publisher and consumer
        self.channel._publisher = Mock()
        self.channel._consumer = Mock()

    def test_basic_consume(self):
        self.channel._get = Mock()
        self.channel.prefetch_limit = 10
        self.channel.prefetched = 5

        self.channel.basic_consume(self.queue_name, True, None, None)
        self.channel.hub.call_soon.assert_called_once_with(self.channel._get, self.queue_name)

    def test_put(self):
        message = {'foo': 'bar'}
        self.channel._publisher.publish = Mock()

        self.channel._put(self.queue_name, message)
        self.channel._publisher.publish.assert_called_with(str(0), dumps(message))

        message = {'bar': 'foo'}
        self.channel._put(self.queue_name, message)
        self.channel._publisher.publish.assert_called_with(str(0), dumps(message))

    def test_get(self):
        delivery_token = '123456'
        message = CheramiMessage({'foo': 'bar'})
        results = [[delivery_token, message]]
        self.channel._consumer.receive = Mock(return_value=results)
        self.channel._on_message_ready = Mock()
        self.channel.prefetched = 0
        self.channel.prefetch_limit = 1

        self.channel._get(self.queue_name)
        self.channel._consumer.receive.assert_called_once_with(num_msgs=self.channel.default_message_batch_size)
        self.channel._on_message_ready.assert_called_once_with(self.queue_name, results)

        # test prefetched > prefetch_limit
        self.channel.prefetched = self.channel.prefetch_limit + 1
        self.channel._get(self.queue_name)
        # only called once like before because not allowed to consume
        self.channel._consumer.receive.assert_called_once_with(num_msgs=self.channel.default_message_batch_size)
        self.channel._on_message_ready.assert_called_once_with(self.queue_name, results)

    def test_on_message_ready(self):
        self.channel._get = Mock()
        self.channel._consumer.nack = Mock()
        self.channel.prefetched = 0

        # test regular message
        delivery_token = '123456'
        message = CheramiMessage({'foo': 'bar'})
        results = [[delivery_token, message]]
        self.channel._handle_message = Mock()
        self.channel._on_message_ready(self.queue_name, results)
        self.channel._handle_message.assert_called_with(self.queue_name, {'foo': 'bar'}, delivery_token)
        self.channel.hub.call_soon.assert_called_once_with(self.channel._get, self.queue_name)
        assert self.channel.prefetched == 1

        # test handle message exception and nack
        delivery_token = '654321'
        message = CheramiMessage({'bar': 'foo'})
        results = [[delivery_token, message]]
        self.channel._handle_message = Mock(side_effect=Exception)
        self.channel._on_message_ready(self.queue_name, results)
        self.channel._handle_message.assert_called_with(self.queue_name, {'bar': 'foo'}, delivery_token)
        self.channel._consumer.nack.assert_called_with(delivery_token)
        assert self.channel.prefetched == 1

        # test bad message format exception and nack
        delivery_token = 000000
        message = {'foo': 'foo'}
        results = [[delivery_token, message]]
        self.channel._on_message_ready(self.queue_name, results)
        self.channel._consumer.nack.assert_called_with(delivery_token)
        assert self.channel.prefetched == 1

    def test_handle_message(self):
        # make sure the callback is called
        message = dumps(dict({'foo': 'bar', 'properties': {'delivery_tag': '654321'}}))
        self.channel.connection._deliver = Mock()
        delivery_token = '123456'

        self.channel._handle_message(self.queue_name, message, delivery_token)
        assert self.channel.delivery_map['654321'] == '123456'
        self.channel.connection._deliver.assert_called_once_with(loads(message), self.queue_name)

    def test_basic_ack(self):
        self.channel._consumer.ack = Mock()
        self.channel.delivery_map = dict({'654321': '123456'})
        self.channel.prefetched = 1

        self.channel.basic_ack('654321')
        self.channel._consumer.ack.assert_called_once_with('123456')
        assert len(self.channel.delivery_map) == 0
        assert self.channel.prefetched == 0

