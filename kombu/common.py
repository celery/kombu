"""
kombu.common
============

Common Utilities.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from collections import defaultdict, deque
from functools import partial
from itertools import count

from . import serialization
from .entity import Exchange, Queue
from .log import Log
from .messaging import Consumer as _Consumer
from .utils import uuid

__all__ = ["Broadcast", "entry_to_queue", "maybe_declare", "uuid",
           "itermessages", "send_reply", "isend_reply",
           "collect_replies", "insured", "ipublish"]

declared_entities = defaultdict(lambda: set())
insured_logger = Log("kombu.insurance")


class Broadcast(Queue):
    """Convenience class used to define broadcast queues.

    Every queue instance will have a unique name,
    and both the queue and exchange is configured with auto deletion.

    :keyword name: This is used as the name of the exchange.
    :keyword queue: By default a unique id is used for the queue
       name for every consumer.  You can specify a custom queue
       name here.
    :keyword \*\*kwargs: See :class:`~kombu.entity.Queue` for a list
        of additional keyword arguments supported.

    """

    def __init__(self, name=None, queue=None, **kwargs):
        return super(Broadcast, self).__init__(
                    name=queue or "bcast.%s" % (uuid(), ),
                    **dict({"alias": name,
                            "auto_delete": True,
                            "exchange": Exchange(name, type="fanout"),
                           }, **kwargs))


def maybe_declare(entity, channel, retry=False, **retry_policy):
    if retry:
        return _imaybe_declare(entity, channel, **retry_policy)
    return _maybe_declare(entity, channel)


def _maybe_declare(entity, channel):
    declared = declared_entities[channel.connection.client]
    if not entity.is_bound:
        entity = entity.bind(channel)
    if not entity.can_cache_declaration or entity not in declared:
        entity.declare()
        declared.add(entity)
        return True
    return False


def _imaybe_declare(entity, channel, **retry_policy):
    entity = entity(channel)
    return channel.connection.client.ensure(entity, _maybe_declare,
                             **retry_policy)(entity, channel)


def itermessages(conn, channel, queue, limit=1, timeout=None,
        Consumer=_Consumer, **kwargs):
    acc = deque()

    def on_message(body, message):
        acc.append((body, message))

    with Consumer(channel, [queue], callbacks=[on_message], **kwargs):
        for _ in eventloop(conn, limit=limit, timeout=timeout,
                           ignore_timeouts=True):
            try:
                yield acc.popleft()
            except IndexError:
                pass


def eventloop(conn, limit=None, timeout=None, ignore_timeouts=False):
    """Best practice generator wrapper around ``Connection.drain_events``.

    Able to drain events forever, with a limit, and optionally ignoring
    timeout errors (a timeout of 1 is often used in environments where
    the socket can get "stuck", and is a best practice for Kombu consumers).

    **Examples**

    ``eventloop`` is a generator::

        >>> from kombu.common import eventloop

        >>> it = eventloop(connection, timeout=1, ignore_timeouts=True)
        >>> it.next()   # one event consumed, or timed out.

        >>> for _ in eventloop(connection, timeout=1, ignore_timeouts=True):
        ...     pass  # loop forever.

    It also takes an optional limit parameter, and timeout errors
    are propagated by default::

        for _ in eventloop(connection, limit=1, timeout=1):
            pass

    .. seealso::

        :func:`itermessages`, which is an event loop bound to one or more
        consumers, that yields any messages received.

    """
    for i in limit and xrange(limit) or count():
        try:
            yield conn.drain_events(timeout=timeout)
        except socket.timeout:
            if timeout and not ignore_timeouts:
                raise
        except socket.error:
            pass


def send_reply(exchange, req, msg, producer=None, **props):
    content_type = req.content_type
    serializer = serialization.registry.type_to_name[content_type]
    maybe_declare(exchange, producer.channel)
    producer.publish(msg, exchange=exchange,
            **dict({"routing_key": req.properties["reply_to"],
                    "correlation_id": req.properties.get("correlation_id"),
                    "serializer": serializer},
                    **props))


def isend_reply(pool, exchange, req, msg, props, **retry_policy):
    return ipublish(pool, send_reply,
                    (exchange, req, msg), props, **retry_policy)


def collect_replies(conn, channel, queue, *args, **kwargs):
    no_ack = kwargs.setdefault("no_ack", True)
    received = False
    for body, message in itermessages(conn, channel, queue, *args, **kwargs):
        if not no_ack:
            message.ack()
        received = True
        yield body
    if received:
        channel.after_reply_message_received(queue.name)


def _ensure_errback(exc, interval):
    insured_logger.error(
        "Connection error: %r. Retry in %ss\n" % (exc, interval),
            exc_info=sys.exc_info())


def revive_connection(connection, channel, on_revive=None):
    if on_revive:
        on_revive(channel)


def revive_producer(producer, channel, on_revive=None):
    revive_connection(producer.connection, channel)
    if on_revive:
        on_revive(channel)


def insured(pool, fun, args, kwargs, errback=None, on_revive=None, **opts):
    """Ensures function performing broker commands completes
    despite intermittent connection failures."""
    errback = errback or _ensure_errback

    with pool.acquire(block=True) as conn:
        conn.ensure_connection(errback=errback)
        # we cache the channel for subsequent calls, this has to be
        # reset on revival.
        channel = conn.default_channel
        revive = partial(revive_connection, conn, on_revive=on_revive)
        insured = conn.autoretry(fun, channel, errback=errback,
                                 on_revive=revive, **opts)
        retval, _ = insured(*args, **dict(kwargs, connection=conn))
        return retval


def ipublish(pool, fun, args=(), kwargs={}, errback=None, on_revive=None,
        **retry_policy):
    with pool.acquire(block=True) as producer:
        errback = errback or _ensure_errback
        revive = partial(revive_producer, producer, on_revive=on_revive)
        f = producer.connection.ensure(producer, fun, on_revive=revive,
                                       errback=errback, **retry_policy)
        return f(*args, **dict(kwargs, producer=producer))


def entry_to_queue(queue, **options):
    binding_key = options.get("binding_key") or options.get("routing_key")

    e_durable = options.get("exchange_durable")
    if e_durable is None:
        e_durable = options.get("durable")

    e_auto_delete = options.get("exchange_auto_delete")
    if e_auto_delete is None:
        e_auto_delete = options.get("auto_delete")

    q_durable = options.get("queue_durable")
    if q_durable is None:
        q_durable = options.get("durable")

    q_auto_delete = options.get("queue_auto_delete")
    if q_auto_delete is None:
        q_auto_delete = options.get("auto_delete")

    e_arguments = options.get("exchange_arguments")
    q_arguments = options.get("queue_arguments")
    b_arguments = options.get("binding_arguments")

    exchange = Exchange(options.get("exchange"),
                        type=options.get("exchange_type"),
                        delivery_mode=options.get("delivery_mode"),
                        routing_key=options.get("routing_key"),
                        durable=e_durable,
                        auto_delete=e_auto_delete,
                        arguments=e_arguments)

    return Queue(queue,
                 exchange=exchange,
                 routing_key=binding_key,
                 durable=q_durable,
                 exclusive=options.get("exclusive"),
                 auto_delete=q_auto_delete,
                 no_ack=options.get("no_ack"),
                 queue_arguments=q_arguments,
                 binding_arguments=b_arguments)
