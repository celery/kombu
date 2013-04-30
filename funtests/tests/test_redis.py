from nose import SkipTest

from funtests import transport


class test_redis(transport.TransportCase):
    transport = 'redis'
    prefix = 'redis'

    def before_connect(self):
        try:
            import redis  # noqa
        except ImportError:
            raise SkipTest('redis not installed')

    def after_connect(self, connection):
        client = connection.channel().client
        client.info()

    def test_cant_connect_raises_connection_error(self):
        conn = self.get_connection(port=65534)
        self.assertRaises(conn.connection_errors, conn.connect)
