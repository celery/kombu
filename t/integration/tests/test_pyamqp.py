from __future__ import absolute_import, unicode_literals

from funtests import transport


class test_pyamqp(transport.TransportCase):
    transport = 'pyamqp'
    prefix = 'pyamqp'
