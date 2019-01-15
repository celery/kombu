from funtests import transport

from kombu.tests.case import skip


@skip.unless_module('qpid.messaging')
class test_qpid(transport.TransportCase):
    transport = 'qpid'
    prefix = 'qpid'
