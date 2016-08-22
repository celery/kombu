# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from case import Mock

from kombu.async.aws import connect_sqs

from .case import AWSCase


class test_connect_sqs(AWSCase):

    def test_connection(self):
        x = connect_sqs('AAKI', 'ASAK', http_client=Mock())
        assert x
        assert x.connection
