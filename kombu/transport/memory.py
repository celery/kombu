"""
kombu.transport.memory
======================

In-memory transport.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from Queue import Queue

from kombu.transport import virtual


class Channel(virtual.Channel):
    queues = {}
    do_restore = False

    def _has_queue(self, queue, **kwargs):
        return queue in self.queues

    def _new_queue(self, queue, **kwargs):
        if queue not in self.queues:
            self.queues[queue] = Queue()

    def _get(self, queue, timeout=None):
        return self.queues[queue].get(block=False)

    def _put(self, queue, message, **kwargs):
        self.queues[queue].put(message)

    def _size(self, queue):
        return self.queues[queue].qsize()

    def _delete(self, queue):
        self.queues.pop(queue, None)

    def _purge(self, queue):
        size = self.queues[queue].qsize()
        self.queues[queue].queue.clear()
        return size


class Transport(virtual.Transport):
    Channel = Channel

    #: memory backend state is global.
    state = virtual.BrokerState()
