# -*- coding: utf-8 -*-
from __future__ import absolute_import

from kombu.async.aws import connect_sqs

from .case import AWSCase


class test_connect_sqs(AWSCase):

    def test_connection(self):
        x = connect_sqs('AAKI', 'ASAK')
        self.assertTrue(x)
        self.assertTrue(x.connection)
