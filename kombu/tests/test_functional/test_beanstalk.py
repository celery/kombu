from nose import SkipTest

from kombu.tests.test_functional import transport


class test_beanstalk(transport.TransportCase):
    transport = "beanstalk"
    prefix = "beanstalk"
    event_loop_max = 10

    def after_connect(self, connection):
        connection.channel().client

    def test_basic_get(self):
        raise SkipTest("beanstalk does not support synchronous access.")
