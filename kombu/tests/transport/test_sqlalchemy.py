from __future__ import absolute_import

import tempfile

from kombu.connection import BrokerConnection
from kombu.tests.utils import TestCase

class test_sqlalchemy(TestCase):

    def test_url_parser(self):
        from kombu.transport import sqlalchemy

        tmppath = tempfile.mkdtemp()

        url = "sqlalchemy+sqlite://{path}/celerydb.sqlite".format(path=tmppath)
        connection = BrokerConnection(url).connect()

        url = "sqla+sqlite://{path}/celerydb.sqlite".format(path=tmppath)
        connection = BrokerConnection(url).connect()

        # Should prevent regression fixed by f187ccd
        url = "sqlb+sqlite://{path}/celerydb.sqlite".format(path=tmppath)
        with self.assertRaises(KeyError):
            BrokerConnection(url).connect()
