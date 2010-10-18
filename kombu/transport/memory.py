from Queue import Queue

from kombu.transport import virtual


class Channel(virtual.Channel):
    queues = {}
    do_restore = False

    def _new_queue(self, queue, **kwargs):
        if queue not in self.queues:
            self.queues[queue] = Queue()

    def _get(self, queue):
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
