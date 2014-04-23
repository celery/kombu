# -*- coding: utf-8 -*-
from __future__ import absolute_import

from boto.sqs import jsonmessage as _jsonmessage

from .message import BaseAsyncMessage


class AsyncJSONMessage(BaseAsyncMessage, _jsonmessage.JSONMessage):
    pass
