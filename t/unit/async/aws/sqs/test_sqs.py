# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock, patch

from kombu.async.aws.sqs import regions, connect_to_region
from kombu.async.aws.sqs.connection import AsyncSQSConnection

from ..case import AWSCase


class test_connect_to_region(AWSCase):

    def test_when_no_boto_installed(self, patching):
        patching('kombu.async.aws.sqs.boto', None)
        with pytest.raises(ImportError):
            regions()

    def test_using_async_connection(self):
        for region in regions():
            assert region.connection_cls is AsyncSQSConnection

    def test_connect_to_region(self):
        with patch('kombu.async.aws.sqs.regions') as regions:
            region = Mock(name='region')
            region.name = 'us-west-1'
            regions.return_value = [region]
            conn = connect_to_region('us-west-1', kw=3.33)
            assert conn is region.connect.return_value
            region.connect.assert_called_with(kw=3.33)

            assert connect_to_region('foo') is None
