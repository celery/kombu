# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from kombu.async.aws.sqs import regions, connect_to_region
from kombu.async.aws.sqs.connection import AsyncSQSConnection

from kombu.tests.async.aws.case import AWSCase
from kombu.tests.case import Mock, patch, set_module_symbol


class test_connect_to_region(AWSCase):

    def test_when_no_boto_installed(self):
        with set_module_symbol('kombu.async.aws.sqs', 'boto', None):
            with self.assertRaises(ImportError):
                regions()

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
