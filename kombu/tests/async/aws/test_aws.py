# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from kombu.async.aws import connect_sqs
from kombu.tests.case import Mock

from .case import AWSCase


class test_connect_sqs(AWSCase):

    def test_connection(self):
        x = connect_sqs('AAKI', 'ASAK', http_client=Mock())
        self.assertTrue(x)
        self.assertTrue(x.connection)
