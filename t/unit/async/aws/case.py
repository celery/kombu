# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import pytest

from case import skip


@skip.if_pypy()
@skip.unless_module('boto3')
@skip.unless_module('pycurl')
@pytest.mark.usefixtures('hub')
class AWSCase(object):
    pass
