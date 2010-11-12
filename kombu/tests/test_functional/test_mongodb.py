from nose import SkipTest

from kombu.tests.test_functional import transport


class test_mongodb(transport.TransportCase):
    transport = "mongodb"
    prefix = "mongodb"
    event_loop_max = 100

    def after_connect(self, connection):
        connection.channel().client

    #def test_basic_get(self):
    #   raise SkipTest("beanstalk does not support synchronous access.")
