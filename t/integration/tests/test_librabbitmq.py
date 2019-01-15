from funtests import transport

from kombu.tests.case import skip


@skip.unless_module('librabbitmq')
class test_librabbitmq(transport.TransportCase):
    transport = 'librabbitmq'
    prefix = 'librabbitmq'
