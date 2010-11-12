from nose import SkipTest

from funtests import transport


class test_beanstalk(transport.TransportCase):
    transport = "beanstalk"
    prefix = "beanstalk"
    event_loop_max = 10

    def after_connect(self, connection):
        connection.channel().client
