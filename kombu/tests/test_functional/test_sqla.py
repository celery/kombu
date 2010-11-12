from nose import SkipTest

from kombu.tests.test_functional import transport


class test_sqla(transport.TransportCase):
    transport = "sqlalchemy"
    prefix = "sqlalchemy"
    event_loop_max = 10
    connection_options = {"hostname": "sqlite://"}

    def before_connect(self):
        try:
            import sqlakombu
        except ImportError:
            raise SkipTest("sqlalchemy-kombu not installed")
