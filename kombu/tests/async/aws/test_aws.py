# -*- coding: utf-8 -*-
from __future__ import absolute_import

from kombu.async.aws import connect_sqs

from kombu.tests.case import HubCase


class test_connect_sqs(HubCase):

    def test_connection(self):
        x = connect_sqs('AAKI', 'ASAK')
        self.assertTrue(x)
        self.assertTrue(x.connection)
