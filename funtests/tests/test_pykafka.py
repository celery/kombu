from nose import SkipTest

from funtests import transport


import logging
logging.basicConfig()
logging.getLogger('pykafka.producer').setLevel(logging.DEBUG)


class test_pykafka(transport.TransportCase):
    transport = 'pykafka'
    prefix = 'pykafka'
    message_size_limit = 100000
    reliable_purge = False

    def before_connect(self):
        try:
            import pykafka # noqa
        except ImportError:
            raise SkipTest('pykafka not installed')

    def purge(self, *args, **kwargs):
        return super(test_pykafka, self).purge(ensure=False, *args, **kwargs)
