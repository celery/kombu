from nose import SkipTest

from funtests import transport


class test_qpid(transport.TransportCase):
    transport = 'qpid'
    prefix = 'qpid'

    def before_connect(self):
        try:
            import qpid.messaging  # noqa
        except ImportError:
            raise SkipTest('qpid.messaging not installed')
