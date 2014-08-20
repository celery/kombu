# -*- coding: utf-8 -*-
from __future__ import absolute_import

from kombu.tests.case import HubCase, case_requires, case_no_pypy


@case_no_pypy
@case_requires('boto', 'pycurl')
class AWSCase(HubCase):
    pass
