"""
kombu.transport.beanstalk
=========================

Beanstalk transport.

:copyright: (c) 2010 - 2012 by David Ziegler.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import socket

from Queue import Empty

from anyjson import loads, dumps
from beanstalkc import Connection, BeanstalkcException, SocketError

from kombu.exceptions import StdChannelError

from . import virtual

DEFAULT_PORT = 11300

__author__ = "David Ziegler <david.ziegler@gmail.com>"


class Channel(virtual.Channel):
    _client = None

    def _parse_job(self, job):
        item, dest = None, None
        if job:
            try:
                item = loads(job.body)
                dest = job.stats()["tube"]
            except Exception:
                job.bury()
            else:
                job.delete()
        else:
            raise Empty()
        return item, dest

    def _put(self, queue, message, **kwargs):
        extra = {}
        priority = message["properties"]["delivery_info"]["priority"]
        ttr = message["properties"].get("ttr")
        if ttr is not None:
            extra["ttr"] = ttr

        self.client.use(queue)
        self.client.put(dumps(message), priority=priority, **extra)

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

        [self.client.ignore(active)
            for active in watching
                if active not in queues]

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
        port = conninfo.port or DEFAULT_PORT
        conn = Connection(host=conninfo.hostname, port=port)
        conn.connect()
        return conn

    def close(self):
        if self._client is not None:
            return self._client.close()
        super(Channel, self).close()

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(virtual.Transport):
    Channel = Channel

    polling_interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (socket.error,
                         SocketError,
                         IOError)
    channel_errors = (StdChannelError,
                      socket.error,
                      IOError,
                      SocketError,
                      BeanstalkcException)
