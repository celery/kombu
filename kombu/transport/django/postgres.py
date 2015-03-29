"""
kombu.transport.django_postgres
=====================

Django database transport optimized for PostgreSQL
 using advisory locks to hide messages that are held
 by a worker.

Inspired by `que` by Chris Hanks: https://github.com/chanks/que
"""
from __future__ import absolute_import

from kombu.transport import virtual

from . import Channel as DjangoChannel, Transport as DjangoTransport
from .models import Message

try:
    from django.apps import AppConfig
except ImportError:  # pragma: no cover
    pass
else:
    class KombuAppConfig(AppConfig):
        name = 'kombu.transport.django.postgres'
        label = name.replace('.', '_')
        verbose_name = 'Message queue'

    default_app_config = 'kombu.transport.django.postgres.KombuAppConfig'


class QoS(virtual.QoS):
    """
    QoS that uses PostgreSQL advisory locks to reserve messages
    """
    restore_at_shutdown = False

    def ack(self, delivery_tag):
        """Acknowledge message and remove from transactional state."""
        self._delivered[delivery_tag].ack()
        self._quick_ack(delivery_tag)

    def reject(self, delivery_tag, requeue=False):
        """Ack or reject and remove from transactional state."""
        self._delivered[delivery_tag].reject(requeue)
        self._quick_ack(delivery_tag)


class Channel(DjangoChannel):
    QoS = QoS

    def _get(self, queue, **kwargs):
        return super(Channel, self)._get(queue, 'pg_objects', **kwargs)

    def _restore(self, message):
        raise NotImplementedError('Should never be called. Use qos.reject instead')

    def basic_reject(self, delivery_tag, requeue=False):
        Message.pg_objects.reject(delivery_tag)

    def basic_ack(self, delivery_tag):
        Message.pg_objects.ack(delivery_tag)


class Transport(DjangoTransport):
    Channel = Channel

    polling_interval = 1
    driver_type = 'sql'
    driver_name = 'postgres'
