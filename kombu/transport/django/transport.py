"""Django Transport module for kombu.

Kombu transport using Django ORM as the message store.

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: Yes
* Supports TTL: Yes

Connection String
=================

.. code-block::

    django:///
"""

from __future__ import annotations

import json
import logging
from queue import Empty

from django.db import transaction

from kombu.transport import virtual
from kombu.transport.django.models import Binding, Exchange, Message, Queue

VERSION = (0, 0, 1)
__version__ = ".".join(map(str, VERSION))

logger = logging.getLogger(__name__)


class Channel(virtual.Channel):
    """The channel class."""

    supports_fanout = True

    def _open(self):
        pass

    def _put(self, queue, message, priority=0, ttl=None, **kwargs):
        queue_instance, _ = Queue.objects.get_or_create(name=queue)
        queue_instance.messages.create(
            message=json.dumps(message), priority=priority, ttl=ttl
        )

    def _get(self, queue, timeout=None):
        with transaction.atomic():
            try:
                queue_instance = Queue.objects.get(name=queue)
            except Queue.DoesNotExist:
                raise Empty()
            message_instance = (
                Message.objects.select_for_update(skip_locked=True)
                .filter(visible=True, queue=queue_instance)
                .order_by("priority", "sent_at", "id")
                .first()
            )
            if message_instance is not None:
                if message_instance.is_expired():
                    message_instance.visible = False
                    message_instance.save(update_fields=["visible"])
                    logger.debug(
                        f"Message with ID {message_instance.id} has expired and is discarded."
                    )
                    return self._get(queue, timeout=timeout)

                message_instance.visible = False
                message_instance.save(update_fields=["visible"])
                msg = message_instance.message
                return json.loads(msg)
            raise Empty()

    def _purge(self, queue):
        try:
            queue_instance = Queue.objects.get(name=queue)
        except Queue.DoesNotExist:
            return
        queue_instance.messages.all().delete()

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        queue_instance, _ = Queue.objects.get_or_create(name=queue)
        exchange_instance, _ = Exchange.objects.get_or_create(name=exchange)
        binding, created = Binding.objects.get_or_create(
            queue=queue_instance,
            exchange=exchange_instance,
            routing_key=routing_key,
        )
        if created:
            logger.debug(f"Binding created: {binding}")
        else:
            logger.debug(f"Binding already exists: {binding}")

    def _put_fanout(self, exchange, message, routing_key, priority=0, **kwargs):
        try:
            exchange_instance = Exchange.objects.get(name=exchange)
        except Exchange.DoesNotExist:
            return
        queues = Queue.objects.filter(
            bindings__exchange=exchange_instance, bindings__routing_key=routing_key
        )
        logger.debug(
            f"Found {len(queues)} queues bound to fanout exchange {exchange_instance.name}"
        )
        for queue in queues:
            # Publish the message to each bound queue
            logger.debug(f"Publishing message to fanout queue: {queue.name}")
            self._put(queue.name, message, priority=priority)

    def get_table(self, exchange):
        try:
            exchange_instance = Exchange.objects.get(name=exchange)
        except Exchange.DoesNotExist:
            return []
        bindings = exchange_instance.bindings.all()
        return [(binding.routing_key, "", binding.queue.name) for binding in bindings]


class Transport(virtual.Transport):
    """The transport class."""

    Channel = Channel

    can_parse_url = True
    driver_type = "django"
    driver_name = "django"

    implements = virtual.Transport.implements.extend(
        exchange_type=frozenset(["direct", "topic", "fanout"])
    )
