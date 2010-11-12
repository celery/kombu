from funtests import transport


class test_couchdb(transport.TransportCase):
    transport = "couchdb"
    prefix = "couchdb"
    event_loop_max = 100

    def after_connect(self, connection):
        connection.channel().client
