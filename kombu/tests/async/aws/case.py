# -*- coding: utf-8 -*-
from __future__ import absolute_import

from kombu.tests.case import HubCase, SkipTest

try:
    import boto
except ImportError:  # pragma: no cover
    boto = None  # noqa


class AWSCase(HubCase):

    def setUp(self):
        if boto is None:
            raise SkipTest('boto is not installed')
        super(AWSCase, self).setUp()
