import os

from nose import SkipTest

from funtests import transport


class test_SQS(transport.TransportCase):
    transport = 'SQS'
    prefix = 'sqs'
    event_loop_max = 100
    message_size_limit = 4192  # SQS max body size / 2.
    reliable_purge = False
    #: does not guarantee FIFO order, even in simple cases
    suppress_disorder_warning = True

    def before_connect(self):
        try:
            import boto  # noqa
        except ImportError:
            raise SkipTest('boto not installed')
        if 'AWS_ACCESS_KEY_ID' not in os.environ:
            raise SkipTest('Missing envvar AWS_ACCESS_KEY_ID')
        if 'AWS_SECRET_ACCESS_KEY' not in os.environ:
            raise SkipTest('Missing envvar AWS_SECRET_ACCESS_KEY')

    def after_connect(self, connection):
        connection.channel().sqs
