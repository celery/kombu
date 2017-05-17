from __future__ import absolute_import, unicode_literals

from funtests import transport

from kombu.tests.case import skip


@skip.unless_module('sqlalchemy')
class test_sqla(transport.TransportCase):
    transport = 'sqlalchemy'
    prefix = 'sqlalchemy'
    event_loop_max = 10
    connection_options = {'hostname': 'sqla+sqlite://'}
