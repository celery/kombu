
from funtests import transport
from nose import SkipTest
import os


class test_SLMQ(transport.TransportCase):
    transport = "SLMQ"
    prefix = "slmq"
    event_loop_max = 100
    message_size_limit = 4192
    reliable_purge = False
    suppress_disorder_warning = True  # does not guarantee FIFO order,
                                      # even in simple cases.

    def before_connect(self):
        if "SLMQ_ACCOUNT" not in os.environ:
            raise SkipTest("Missing envvar SLMQ_ACCOUNT")
        if "SL_USERNAME" not in os.environ:
            raise SkipTest("Missing envvar SL_USERNAME")
        if "SL_API_KEY" not in os.environ:
            raise SkipTest("Missing envvar SL_API_KEY")
        if "SLMQ_HOST" not in os.environ:
            raise SkipTest("Missing envvar SLMQ_HOST")
        if "SLMQ_SECURE" not in os.environ:
            raise SkipTest("Missing envvar SLMQ_SECURE")

    def after_connect(self, connection):
        pass
