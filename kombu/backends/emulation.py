from carrot.backends.base import BaseBackend, BaseMessage
from anyjson import serialize, deserialize
from itertools import count
from django.utils.datastructures import SortedDict
from carrot.utils import gen_unique_id
import sys
import time
import atexit

from Queue import Empty as QueueEmpty
from itertools import cycle


class QueueSet(object):
    """A set of queues that operates as one."""

    def __init__(self, backend, queues):
        self.backend = backend
        self.queues = queues

        # an infinite cycle through all the queues.
        self.cycle = cycle(self.queues)

        # A set of all the queue names, so we can match when we've
        # tried all of them.
        self.all = frozenset(self.queues)

    def get(self):
        """Get the next message avaiable in the queue.

        :returns: The message and the name of the queue it came from as
            a tuple.
        :raises Empty: If there are no more items in any of the queues.

        """

        # A set of queues we've already tried.
        tried = set()

        while True:
            # Get the next queue in the cycle, and try to get an item off it.
            queue = self.cycle.next()
            try:
                item = self.backend._get(queue)
            except Empty:
                # raises Empty when we've tried all of them.
                tried.add(queue)
                if tried == self.all:
                    raise
            else:
                return item, queue

    def __repr__(self):
        return "<QueueSet: %s>" % repr(self.queue_names)


class QualityOfService(object):

    def __init__(self, resource, prefetch_count=None, interval=None):
        self.resource = resource
        self.prefetch_count = prefetch_count
        self.interval = interval
        self._delivered = SortedDict()
        self._restored_once = False
        atexit.register(self.restore_unacked_once)

    def can_consume(self):
        return len(self._delivered) > self.prefetch_count

    def append(self, message, queue_name, delivery_tag):
        self._delivered[delivery_tag] = message, queue_name

    def ack(self, delivery_tag):
        self._delivered.pop(delivery_tag, None)

    def restore_unacked(self):
        for message, queue_name in self._delivered.items():
            self.resource.put(queue_name, message)
        self._delivered = SortedDict()

    def requeue(self, delivery_tag):
        try:
            message, queue_name = self._delivered.pop(delivery_tag)
        except KeyError:
            pass
        self.resource.put(queue_name, message)

    def restore_unacked_once(self):
        if not self._restored_once:
            if self._delivered:
                sys.stderr.write(
                    "Restoring unacknowledged messages: %s\n" % (
                        self._delivered))
            self.restore_unacked()
            if self._delivered:
                sys.stderr.write("UNRESTORED MESSAGES: %s\n" % (
                    self._delivered))


class Message(BaseMessage):

    def __init__(self, backend, payload, **kwargs):
        self.backend = backend

        payload = deserialize(payload)
        kwargs["body"] = payload.get("body").encode("utf-8")
        kwargs["delivery_tag"] = payload.get("delivery_tag")
        kwargs["content_type"] = payload.get("content-type")
        kwargs["content_encoding"] = payload.get("content-encoding")
        kwargs["priority"] = payload.get("priority")
        self.destination = payload.get("destination")

        super(Message, self).__init__(backend, **kwargs)

    def reject(self):
        raise NotImplementedError(
            "This backend does not implement basic.reject")


class EmulationBase(BaseBackend):
    Message = Message
    default_port = None
    interval = 1
    _prefetch_count = None

    QueueSet = QueueSet

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self._consumers = {}
        self._callbacks = {}
        self._qos_manager = None

    def establish_connection(self):
        return self # for drain events

    def close_connection(self, connection):
        pass

    def queue_exists(self, queue):
        return True

    def queue_purge(self, queue, **kwargs):
        return self._purge(queue, **kwargs)

    def _poll(self, resource):
        while True:
            if self.qos_manager.can_consume():
                try:
                    return resource.get()
                except QueueEmpty:
                    pass
            time.sleep(self.interval)

    def declare_consumer(self, queue, no_ack, callback, consumer_tag,
                         **kwargs):
        self._consumers[consumer_tag] = queue
        self._callbacks[queue] = callback

    def drain_events(self, timeout=None):
        queueset = self.QueueSet(self._consumers.values())
        payload, queue = self._poll(queueset)

        if not queue or queue not in self._callbacks:
            return

        self._callbacks[queue](payload)

    def consume(self, limit=None):
        for total_message_count in count():
            if limit and total_message_count >= limit:
                raise StopIteration

            self.drain_events()

            yield True

    def queue_declare(self, queue, *args, **kwargs):
        pass

    def _get_many(self, queues):
        raise NotImplementedError("Emulations must implement _get_many")

    def _get(self, queue):
        raise NotImplementedError("Emulations must implement _get")

    def _put(self, queue, message):
        raise NotImplementedError("Emulations must implement _put")

    def _purge(self, queue, message):
        raise NotImplementedError("Emulations must implement _purge")

    def get(self, queue, **kwargs):
        try:
            payload = self._get(queue)
        except QueueEmpty:
            return None
        else:
            return self.message_to_python(payload)

    def ack(self, delivery_tag):
        self.qos_manager.ack(delivery_tag)

    def requeue(self, delivery_tag):
        self.qos_manager.requeue(delivery_tag)

    def message_to_python(self, raw_message):
        message = self.Message(backend=self, payload=raw_message)
        self.qos_manager.append(message, message.destination,
                                message.delivery_tag)
        return message

    def prepare_message(self, message_data, delivery_mode, priority=0,
            content_type=None, content_encoding=None):
        return {"body": message_data,
                "priority": priority or 0,
                "content-encoding": content_encoding,
                "content-type": content_type}

    def publish(self, message, exchange, routing_key, **kwargs):
        message["destination"] = exchange
        self._put(exchange, message)

    def cancel(self, consumer_tag):
        queue = self._consumers.pop(consumer_tag, None)
        self._callbacks.pop(queue, None)

    def close(self):
        for consumer_tag in self._consumers.keys():
            self.cancel(consumer_tag)

    def basic_qos(self, prefetch_size, prefetch_count, apply_global=False):
        self._prefetch_count = prefetch_count

    @property
    def qos_manager(self):
        if self._qos_manager is None:
            self._qos_manager = QualityOfService(self)

        # Update prefetch count / interval
        self._qos_manager.prefetch_count = self._prefetch_count
        self._qos_manager.interval = self.interval

        return self._qos_manager
