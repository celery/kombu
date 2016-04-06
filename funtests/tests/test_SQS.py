from __future__ import absolute_import, unicode_literals

from funtests import transport

from kombu.tests.case import skip


@skip.unless_environ('AWS_ACCESS_KEY_ID')
@skip.unless_environ('AWS_SECRET_ACCESS_KEY')
@skip.unless_module('boto')
class test_SQS(transport.TransportCase):
    transport = 'SQS'
    prefix = 'sqs'
    event_loop_max = 100
    message_size_limit = 4192  # SQS max body size / 2.
    reliable_purge = False
    #: does not guarantee FIFO order, even in simple cases
    suppress_disorder_warning = True

    def after_connect(self, connection):
        connection.channel().sqs
