from nose import SkipTest

from funtests import transport


class test_zookeeper(transport.TransportCase):
    transport = 'zookeeper'
    prefix = 'zookeeper'
    event_loop_max = 100

    def before_connect(self):
        try:
            import kazoo  # noqa
        except ImportError:
            raise SkipTest('kazoo not installed')

    def after_connect(self, connection):
        connection.channel().client
