from nose import SkipTest

from funtests import transport


class test_librabbitmq(transport.TransportCase):
    transport = 'librabbitmq'
    prefix = 'librabbitmq'

    def before_connect(self):
        try:
            import librabbitmq  # noqa
        except ImportError:
            raise SkipTest('librabbitmq not installed')
