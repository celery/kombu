from __future__ import absolute_import

from kombu.connection import BrokerConnection

from .utils import TestCase, skip_if_not_module


class MockConnection(dict):

    def __setattr__(self, key, value):
        self[key] = value


class test_mongodb(TestCase):

    @skip_if_not_module("pymongo")
    def test_url_parser(self):
        from kombu.transport import mongodb
        from pymongo.errors import ConfigurationError

        class Transport(mongodb.Transport):
            Connection = MockConnection

        url = "mongodb://"
        c = BrokerConnection(url, transport=Transport).connect()
        client = c.channels[0].client
        self.assertEquals(client.name, "kombu_default")
        self.assertEquals(client.connection.host, "127.0.0.1")

        url = "mongodb://localhost"
        c = BrokerConnection(url, transport=Transport).connect()
        client = c.channels[0].client
        self.assertEquals(client.name, "kombu_default")

        url = "mongodb://localhost/dbname"
        c = BrokerConnection(url, transport=Transport).connect()
        client = c.channels[0].client
        self.assertEquals(client.name, "dbname")

        url = "mongodb://localhost,example.org:29017/dbname"
        c = BrokerConnection(url, transport=Transport).connect()
        client = c.channels[0].client

        nodes = client.connection.nodes
        self.assertEquals(len(nodes), 2)
        self.assertTrue(("example.org", 29017) in nodes)
        self.assertEquals(client.name, "dbname")

        # Passing options breaks kombu's _init_params method
        # url = "mongodb://localhost,localhost2:29017/dbname?safe=true"
        # c = BrokerConnection(url, transport=Transport).connect()
        # client = c.channels[0].client

        url = "mongodb://username:password@localhost/dbname"
        c = BrokerConnection(url, transport=Transport).connect()
        # Assuming there's no user 'username' with password 'password'
        # configured in mongodb

        # Needed, otherwise the error would be rose before
        # the assertRaises is called
        def get_client():
            c.channels[0].client
        self.assertRaises(ConfigurationError, get_client)
