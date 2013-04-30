from nose import SkipTest

from funtests import transport


class test_couchdb(transport.TransportCase):
    transport = 'couchdb'
    prefix = 'couchdb'
    event_loop_max = 100

    def before_connect(self):
        try:
            import couchdb  # noqa
        except ImportError:
            raise SkipTest('couchdb not installed')

    def after_connect(self, connection):
        connection.channel().client
