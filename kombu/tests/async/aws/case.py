# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from kombu.tests.case import HubCase, skip


@skip.if_pypy()
@skip.unless_module('boto')
@skip.unless_module('pycurl')
class AWSCase(HubCase):
    pass
