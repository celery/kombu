# -*- coding: utf-8 -*-
"""
    kombu.five
    ~~~~~~~~~~~

    Compatibility implementations of features
    only available in newer Python versions.


"""
from __future__ import absolute_import, unicode_literals

import sys
import vine.five

sys.modules[__name__] = vine.five
