from __future__ import absolute_import, unicode_literals

from funtests import transport

from kombu.tests.case import skip


@skip.unless_module('amqplib')
class test_amqplib(transport.TransportCase):
    transport = 'amqplib'
    prefix = 'amqplib'
