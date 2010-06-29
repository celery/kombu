"""AMQP Messaging Framework for Python"""
VERSION = (0, 10, 5)
__version__ = ".".join(map(str, VERSION))
__author__ = "Ask Solem"
__contact__ = "askh@opera.com"
__homepage__ = "http://github.com/ask/carrot/"
__docformat__ = "restructuredtext"

import os
if not os.environ.get("KOMBU_NO_EVAL", False):
    from kombu.connection import BrokerConnection
    from kombu.entity import Exchange, Binding
    from kombu.messaging import Consumer, Producer
