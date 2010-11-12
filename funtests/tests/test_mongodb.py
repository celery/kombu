from funtests import transport


class test_mongodb(transport.TransportCase):
    transport = "mongodb"
    prefix = "mongodb"
    event_loop_max = 100

    def after_connect(self, connection):
        connection.channel().client
