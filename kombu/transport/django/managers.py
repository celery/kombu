from __future__ import absolute_import

from functools import wraps

from django.db import transaction, connection, models
try:
    from django.db import connections, router
except ImportError:  # pre-Django 1.2
    connections = router = None  # noqa


try:
    transaction.atomic
except AttributeError:
    commit_on_success = transaction.commit_on_success
else:
    def commit_on_success(fun):
        @wraps(fun)
        def _commit(*args, **kwargs):
            with transaction.atomic():
                return fun(*args, **kwargs)
        return _commit

from . import pgsql


class QueueManager(models.Manager):

    def publish(self, queue_name, payload):
        queue, created = self.get_or_create(name=queue_name)
        queue.messages.create(payload=payload)

    def size(self, queue_name):
        return self.get(name=queue_name).messages.count()

    def purge(self, queue_name):
        try:
            queue = self.get(name=queue_name)
        except self.model.DoesNotExist:
            return

        messages = queue.messages.all()
        count = messages.count()
        messages.delete()
        return count


def select_for_update(qs):
    if connection.vendor == 'oracle':
        return qs
    try:
        return qs.select_for_update()
    except AttributeError:
        return qs


class MessageManager(models.Manager):
    _messages_received = [0]
    cleanup_every = 10

    @commit_on_success
    def pop(self, queue):
        try:
            resultset = select_for_update(
                self.filter(visible=True, queue__name=queue).order_by('sent_at', 'id')
            )
            result = resultset[0:1].get()
            result.visible = False
            result.save()
            recv = self.__class__._messages_received
            recv[0] += 1
            if not recv[0] % self.cleanup_every:
                self.cleanup()
            return result
        except self.model.DoesNotExist:
            pass

    def cleanup(self):
        cursor = self.connection_for_write().cursor()
        cursor.execute(
            'DELETE FROM %s WHERE visible=%%s' % (
                self.model._meta.db_table,),
            (False,)
        )

    def connection_for_write(self):
        if connections:
            return connections[router.db_for_write(self.model)]
        return connection


class PostgresMessageManager(MessageManager):
    def pop(self, queue):
        results = self.raw(
            pgsql.LOCK_JOB, dict(app_id=pgsql.APP_ID, queue=queue)
        )
        if list(results):
            return results[0]

    def ack(self, delivery_tag):
        """Delete the message and remove the advisory lock"""
        self.filter(pk=delivery_tag).delete()
        cursor = self.connection_for_write().cursor()
        cursor.execute(pgsql.UNLOCK, dict(app_id=pgsql.APP_ID, lock_id=delivery_tag))

    def reject(self, delivery_tag):
        """Remove the advisory lock without deleting the message"""
        cursor = self.connection_for_write().cursor()
        cursor.execute(pgsql.UNLOCK, dict(app_id=pgsql.APP_ID, lock_id=delivery_tag))
