from __future__ import absolute_import
from __future__ import with_statement

from mock import patch

from kombu.connection import BrokerConnection
from kombu.tests.utils import TestCase


class test_sqlalchemy(TestCase):

    @patch("kombu.transport.sqlalchemy.Channel._open")
    def test_url_parser(self, _open):
        url = "sqlalchemy+sqlite://celerydb.sqlite"
        BrokerConnection(url).connect()

        url = "sqla+sqlite://celerydb.sqlite"
        BrokerConnection(url).connect()

        # Should prevent regression fixed by f187ccd
        url = "sqlb+sqlite://celerydb.sqlite"
        with self.assertRaises(KeyError):
            BrokerConnection(url).connect()
