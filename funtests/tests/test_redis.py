from funtests import transport


class test_redis(transport.TransportCase):
    transport = "redis"
    prefix = "redis"

    def after_connect(self, connection):
        client = connection.channel().client
        client.info()
