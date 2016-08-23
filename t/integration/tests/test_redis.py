from __future__ import absolute_import, unicode_literals

from funtests import transport

from kombu.tests.case import skip


@skip.unless_module('redis')
class test_redis(transport.TransportCase):
    transport = 'redis'
    prefix = 'redis'

    def after_connect(self, connection):
        client = connection.channel().client
        client.info()

    def test_cannot_connect_raises_connection_error(self):
        conn = self.get_connection(port=65534)
        with self.assertRaises(conn.connection_errors):
            conn.connect()
