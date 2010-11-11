from kombu.tests.test_functional import transport


class test_redis(transport.TransportCase):
    transport = "redis"
    prefix = "redis"

    def after_connect(self, connection):
        connection.channel().client
