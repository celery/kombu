"""
kombu.transport.beanstalk
=========================

Beanstalk transport.

:copyright: (c) 2010 by David Ziegler.
:license: BSD, see LICENSE for more details.

"""
import socket

from Queue import Empty

from anyjson import serialize, deserialize
from beanstalkc import Connection, BeanstalkcException, SocketError

from kombu.transport import virtual

DEFAULT_PORT = 11300

__author__ = "David Ziegler <david.ziegler@gmail.com>"


class Channel(virtual.Channel):
    _client = None

    def _parse_job(self, job):
        item, dest = None, None
        if job:
            try:
                item = deserialize(job.body)
                dest = job.stats()["tube"]
            except Exception:
                job.bury()
            else:
                job.delete()
        else:
            raise Empty()
        return item, dest

    def _put(self, queue, message, **kwargs):
        priority = message["properties"]["priority"]
        self.client.use(queue)
        self.client.put(serialize(message), priority=priority)

    def _get(self, queue):
        if queue not in self.client.watching():
            self.client.watch(queue)

        [self.client.ignore(active)
            for active in self.client.watching()
                if active != queue]

        job = self.client.reserve(timeout=1)
        item, dest = self._parse_job(job)
        return item

    def _get_many(self, queues, timeout=1):
        # timeout of None will cause beanstalk to timeout waiting
        # for a new request
        if timeout is None:
            timeout = 1

        watching = self.client.watching()
        [self.client.watch(active)
            for active in queues
                if active not in watching]

        job = self.client.reserve(timeout=timeout)
        return self._parse_job(job)

    def _purge(self, queue):
        if queue not in self.client.watching():
            self.client.watch(queue)

        [self.client.ignore(active)
                for active in self.client.watching()
                    if active != queue]
        count = 0
        while 1:
            job = self.client.reserve(timeout=1)
            if job:
                job.delete()
                count += 1
            else:
                break
        return count

    def _size(self, queue):
        return 0

    def _open(self):
        conninfo = self.connection.client
        return Connection(host=conninfo.hostname, port=conninfo.port)

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(virtual.Transport):
    Channel = Channel

    interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (socket.error,
                         SocketError,
                         IOError)
    channel_errors = (socket.error,
                      IOError,
                      SocketError,
                      BeanstalkcException)
