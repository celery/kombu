from Queue import Queue

from kombu.backends import emulation


class MemoryChannel(emulation.Channel):
    queues = {}
    do_restore = False

    def _new_queue(self, queue, **kwargs):
        self.queues[queue] = Queue()

    def _get(self, queue):
        return self.queues[queue].get(block=False)

    def _put(self, queue, message):
        self.queues[queue].put(message)

    def _purge(self, queue):
        size = self.queues[queue].qsize()
        self.queues[queue].queue.clear()
        return size


class MemoryBackend(emulation.EmulationBase):
    Channel = MemoryChannel
