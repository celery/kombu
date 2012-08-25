"""
kombu.transport
===============

Built-in transports.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys

from kombu.syn import _detect_environment


def supports_librabbitmq():
    if _detect_environment() == 'default':
        try:
            import librabbitmq  # noqa
            return True
        except ImportError:
            pass
    return False


def _ghettoq(name, new, alias=None):
    xxx = new   # stupid enclosing

    def __inner():
        import warnings
        _new = callable(xxx) and xxx() or xxx
        gtransport = 'ghettoq.taproot.%s' % name
        ktransport = 'kombu.transport.%s.Transport' % _new
        this = alias or name
        warnings.warn("""
    Ghettoq does not work with Kombu, but there is now a built-in version
    of the %s transport.

    You should replace %r with simply: %r
        """ % (name, gtransport, this))
        return ktransport

    return __inner


TRANSPORT_ALIASES = {
    'amqp': 'kombu.transport.amqplib.Transport',
    'pyamqp': 'kombu.transport.pyamqp.Transport',
    'amqplib': 'kombu.transport.amqplib.Transport',
    'librabbitmq': 'kombu.transport.librabbitmq.Transport',
    'pika': 'kombu.transport.pika2.Transport',
    'oldpika': 'kombu.transport.pika.SyncTransport',
    'memory': 'kombu.transport.memory.Transport',
    'redis': 'kombu.transport.redis.Transport',
    'SQS': 'kombu.transport.SQS.Transport',
    'sqs': 'kombu.transport.SQS.Transport',
    'beanstalk': 'kombu.transport.beanstalk.Transport',
    'mongodb': 'kombu.transport.mongodb.Transport',
    'couchdb': 'kombu.transport.couchdb.Transport',
    'zookeeper': 'kombu.transport.zookeeper.Transport',
    'django': 'kombu.transport.django.Transport',
    'sqlalchemy': 'kombu.transport.sqlalchemy.Transport',
    'sqla': 'kombu.transport.sqlalchemy.Transport',
    'ghettoq.taproot.Redis': _ghettoq('Redis', 'redis', 'redis'),
    'ghettoq.taproot.Database': _ghettoq('Database', 'django', 'django'),
    'ghettoq.taproot.MongoDB': _ghettoq('MongoDB', 'mongodb'),
    'ghettoq.taproot.Beanstalk': _ghettoq('Beanstalk', 'beanstalk'),
    'ghettoq.taproot.CouchDB': _ghettoq('CouchDB', 'couchdb'),
    'filesystem': 'kombu.transport.filesystem.Transport',
    'zeromq': 'kombu.transport.zmq.Transport',
    'zmq': 'kombu.transport.zmq.Transport',
}

_transport_cache = {}


def resolve_transport(transport=None):
    transport = TRANSPORT_ALIASES.get(transport, transport)
    if callable(transport):
        transport = transport()
    transport_module_name, _, transport_cls_name = transport.rpartition('.')
    if not transport_module_name:
        raise KeyError('No such transport: %s' % (transport, ))
    return transport_module_name, transport_cls_name


def _get_transport_cls(transport=None):
    transport_module_name, transport_cls_name = resolve_transport(transport)
    __import__(transport_module_name)
    transport_module = sys.modules[transport_module_name]
    return getattr(transport_module, transport_cls_name)


def get_transport_cls(transport=None):
    """Get transport class by name.

    The transport string is the full path to a transport class, e.g.::

        "kombu.transport.amqplib.Transport"

    If the name does not include `"."` (is not fully qualified),
    the alias table will be consulted.

    """
    if transport not in _transport_cache:
        _transport_cache[transport] = _get_transport_cls(transport)
    return _transport_cache[transport]
