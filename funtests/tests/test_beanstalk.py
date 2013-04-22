from funtests import transport

from nose import SkipTest


class test_beanstalk(transport.TransportCase):
    transport = 'beanstalk'
    prefix = 'beanstalk'
    event_loop_max = 10
    message_size_limit = 47662

    def before_connect(self):
        try:
            import beanstalkc  # noqa
        except ImportError:
            raise SkipTest('beanstalkc not installed')

    def after_connect(self, connection):
        connection.channel().client
