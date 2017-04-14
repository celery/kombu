from __future__ import absolute_import, unicode_literals

from case import skip

from kombu import Connection
from kombu.transport import zookeeper


@skip.unless_module('kazoo')
class test_Channel:
    def setup(self):
        self.connection = self.create_connection()
        self.channel = self.connection.default_channel

    def create_connection(self, **kwargs):
        return Connection(transport=zookeeper.Transport, **kwargs)

    def teardown(self):
        self.connection.close()

    def test_put_puts_bytes_to_queue(self):
        class AssertQueue:
            def put(self, value, priority):
                assert isinstance(value, bytes)

        self.channel._queues['foo'] = AssertQueue()
        self.channel._put(queue='foo', message='bar')
