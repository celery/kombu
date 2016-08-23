from __future__ import absolute_import, unicode_literals

from funtests import transport

from kombu.tests.case import skip


@skip.unless_environ('SLMQ_ACCOUNT')
@skip.unless_environ('SL_USERNAME')
@skip.unless_environ('SL_API_KEY')
@skip.unless_environ('SLMQ_HOST')
@skip.unless_environ('SLMQ_SECURE')
class test_SLMQ(transport.TransportCase):
    transport = 'SLMQ'
    prefix = 'slmq'
    event_loop_max = 100
    message_size_limit = 4192
    reliable_purge = False
    #: does not guarantee FIFO order, even in simple cases.
    suppress_disorder_warning = True
