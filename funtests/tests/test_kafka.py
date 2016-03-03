from nose import SkipTest

from funtests import transport


import logging
logging.basicConfig()
logging.getLogger('pykafka.producer').setLevel(logging.DEBUG)


class test_kafka(transport.TransportCase):
    transport = 'pykafka'
    prefix = 'pykafka'
    message_size_limit = 100000

    def before_connect(self):
        try:
            import pykafka # noqa
        except ImportError:
            raise SkipTest('pykafka not installed')
