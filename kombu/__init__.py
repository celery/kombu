"""AMQP Messaging Framework for Python"""
VERSION = (0, 9, 5)
__version__ = ".".join(map(str, VERSION))
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://github.com/ask/kombu/"
__docformat__ = "restructuredtext"

import os
if not os.environ.get("KOMBU_NO_EVAL", False):
    from kombu.connection import BrokerConnection
    from kombu.entity import Exchange, Queue
    from kombu.messaging import Consumer, Producer
