# -*- coding: utf-8 -*-
from __future__ import absolute_import

from kombu.async.aws.sqs import regions, connect_to_region
from kombu.async.aws.sqs.connection import AsyncSQSConnection

from kombu.tests.case import HubCase, Mock, patch


class test_connect_to_region(HubCase):

    def test_using_async_connection(self):
        for region in regions():
            self.assertIs(region.connection_cls, AsyncSQSConnection)

    def test_connect_to_region(self):
        with patch('kombu.async.aws.sqs.regions') as regions:
            region = Mock(name='region')
            region.name = 'us-west-1'
            regions.return_value = [region]
            conn = connect_to_region('us-west-1', kw=3.33)
            self.assertIs(conn, region.connect.return_value)
            region.connect.assert_called_with(kw=3.33)

            self.assertIsNone(connect_to_region('foo'))
