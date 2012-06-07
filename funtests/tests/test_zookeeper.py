from funtests import transport


class test_zookeeper(transport.TransportCase):
    transport = "zookeeper"
    prefix = "zookeeper"
    event_loop_max = 100

    def after_connect(self, connection):
        connection.channel().client
