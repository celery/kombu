from nose import SkipTest

from funtests import transport


class test_amqplib(transport.TransportCase):
    transport = 'amqplib'
    prefix = 'amqplib'

    def before_connect(self):
        try:
            import amqplib  # noqa
        except ImportError:
            raise SkipTest('amqplib not installed')
