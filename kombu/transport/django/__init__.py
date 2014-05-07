"""Kombu transport using the Django database as a message store."""
from __future__ import absolute_import

from anyjson import loads, dumps

from django.conf import settings
from django.core import exceptions as errors

from kombu.five import Empty
from kombu.transport import virtual
from kombu.utils.encoding import bytes_to_str

from .models import Queue, Message

VERSION = (1, 0, 0)
__version__ = '.'.join(map(str, VERSION))

POLLING_INTERVAL = getattr(settings, 'KOMBU_POLLING_INTERVAL',
                           getattr(settings, 'DJKOMBU_POLLING_INTERVAL', 5.0))


class Channel(virtual.Channel):

    def _new_queue(self, queue, **kwargs):
        Queue.objects.get_or_create(name=queue)

    def _put(self, queue, message, **kwargs):
        Queue.objects.publish(queue, dumps(message))

    def basic_consume(self, queue, *args, **kwargs):
        qinfo = self.state.bindings[queue]
        exchange = qinfo[0]
        if self.typeof(exchange).type == 'fanout':
            return
        super(Channel, self).basic_consume(queue, *args, **kwargs)

    def basic_publish(self, message, exchange, routing_key, **kwargs):
        try:
            content_type = message['content-type']

            if content_type == 'application/json':
                # parse the body to see what kind of message this is
                body = loads(message['body'])

                # try and execute this method on the channel, else
                # continue with the default action of basic_publish
                #
                # Methods are of the form 'do_revoke_method' where revoke
                # is the name of the method
                method = getattr(self, 'do_%s_method' % body['method'])
                arguments = body['arguments']

                # FIXME: this method should raise like normal
                return method(routing_key, **arguments)

        except (KeyError, AttributeError):
            pass

        return super(Channel, self).basic_publish(message, exchange,
                                                  routing_key, **kwargs)

    def _get(self, queue):
        #self.refresh_connection()
        m = Queue.objects.fetch(queue)
        if m:
            return loads(bytes_to_str(m))
        raise Empty()

    def _size(self, queue):
        return Queue.objects.size(queue)

    def _purge(self, queue):
        return Queue.objects.purge(queue)

    def refresh_connection(self):
        from django import db
        db.close_connection()

    def do_revoke_method(self, queue, task_id=None, terminate=False, **kwargs):
        """
        Revoke some jobs
        """

        if terminate:
            raise NotImplementedError("terminate flag is not implemented")

        task_id = task_id or []

        # drop the messages from the queue
        # this requires Postgres 9.3
        # FIXME: raise NotImplementedError on older Postgres
        Message.objects.extra(
            where=["payload::json->'properties'->>'correlation_id' in %s"],
            params=[tuple(task_id)])\
            .delete()

        # inform Celery we've revoked the task in case anyone is waiting-
        # use store_result so that it creates the task entry if required
        from djcelery.models import TaskMeta
        from celery import states
        from celery.exceptions import TaskRevokedError

        for task in task_id:
            # FIXME: shouldn't overwrite finished tasks
            TaskMeta.objects.store_result(task, TaskRevokedError,
                                          states.REVOKED)


class Transport(virtual.Transport):
    Channel = Channel

    default_port = 0
    polling_interval = POLLING_INTERVAL
    channel_errors = (
        virtual.Transport.channel_errors + (
            errors.ObjectDoesNotExist, errors.MultipleObjectsReturned)
    )
    driver_type = 'sql'
    driver_name = 'django'

    def driver_version(self):
        import django
        return '.'.join(map(str, django.VERSION))
