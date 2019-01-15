import pytest
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

    @pytest.mark.parametrize('input,expected', (
        ('', '/'),
        ('/root', '/root'),
        ('/root/', '/root'),
    ))
    def test_virtual_host_normalization(self, input, expected):
        with self.create_connection(virtual_host=input) as conn:
            assert conn.default_channel._vhost == expected
