from __future__ import absolute_import, unicode_literals

from funtests import transport

from kombu.tests.case import skip


@skip.unless_module('couchdb')
class test_couchdb(transport.TransportCase):
    transport = 'couchdb'
    prefix = 'couchdb'
    event_loop_max = 100

    def after_connect(self, connection):
        connection.channel().client
