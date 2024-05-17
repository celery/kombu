from __future__ import annotations

import pytest
from tembo_pgmq_python import __version__ as pgmq_version

from kombu import Connection
from kombu.transport.pgmq import PGMQTransport

pytest.importorskip('tembo_pgmq_python')

class test_Channel:
    def setup_method(self):
        self.connection = self.create_connection()
        self.channel = self.connection.default_channel

    def create_connection(self, **kwargs):
        return Connection(transport=PGMQTransport, **kwargs)

    def teardown_method(self):
        self.connection.close()

    def test_put_puts_bytes_to_queue(self):
        class AssertQueue:
            def put(self, value, priority):
                assert isinstance(value, bytes)

        self.channel._queues['foo'] = AssertQueue()
        self.channel._put(queue='foo', message={'body': 'bar'})

    @pytest.mark.parametrize('input,expected', (
        ('', '/'),
        ('/root', '/root'),
        ('/root/', '/root'),
    ))
    def test_virtual_host_normalization(self, input, expected):
        with self.create_connection(virtual_host=input) as conn:
            assert conn.default_channel._vhost == expected

    def test_driver_version(self):
        transport = PGMQTransport()
        assert transport.driver_version() == pgmq_version
