from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock

from kombu import Connection, Exchange, Queue


class SimpleBase:

    def Queue(self, name, *args, **kwargs):
        q = name
        if not isinstance(q, Queue):
            q = self.__class__.__name__
            if name:
                q = '%s.%s' % (q, name)
        return self._Queue(q, *args, **kwargs)

    def _Queue(self, *args, **kwargs):
        raise NotImplementedError()

    def setup(self):
        self.connection = Connection(transport='memory')
        self.connection.default_channel.exchange_declare('amq.direct')
        self.q = self.Queue(None, no_ack=True)

    def teardown(self):
        self.q.close()
        self.connection.close()
        self.connection = None
        self.q = None

    def test_produce__consume(self):
        q = self.Queue('test_produce__consume', no_ack=True)

        q.put({'hello': 'Simple'})

        assert q.get(timeout=1).payload == {'hello': 'Simple'}
        with pytest.raises(q.Empty):
            q.get(timeout=0.1)

    def test_produce__basic_get(self):
        q = self.Queue('test_produce__basic_get', no_ack=True)
        q.put({'hello': 'SimpleSync'})
        assert q.get_nowait().payload == {'hello': 'SimpleSync'}
        with pytest.raises(q.Empty):
            q.get_nowait()

        q.put({'hello': 'SimpleSync'})
        assert q.get(block=False).payload == {'hello': 'SimpleSync'}
        with pytest.raises(q.Empty):
            q.get(block=False)

    def test_clear(self):
        q = self.Queue('test_clear', no_ack=True)

        for i in range(10):
            q.put({'hello': 'SimplePurge%d' % (i,)})

        assert q.clear() == 10

    def test_enter_exit(self):
        q = self.Queue('test_enter_exit')
        q.close = Mock()

        assert q.__enter__() is q
        q.__exit__()
        q.close.assert_called_with()

    def test_qsize(self):
        q = self.Queue('test_clear', no_ack=True)

        for i in range(10):
            q.put({'hello': 'SimplePurge%d' % (i,)})

        assert q.qsize() == 10
        assert len(q) == 10

    def test_autoclose(self):
        channel = self.connection.channel()
        q = self.Queue('test_autoclose', no_ack=True, channel=channel)
        q.close()

    def test_custom_Queue(self):
        n = self.__class__.__name__
        exchange = Exchange('%s-test.custom.Queue' % (n,))
        queue = Queue('%s-test.custom.Queue' % (n,),
                      exchange,
                      'my.routing.key')

        q = self.Queue(queue)
        assert q.consumer.queues[0] == queue
        q.close()

    def test_bool(self):
        q = self.Queue('test_nonzero')
        assert q


class test_SimpleQueue(SimpleBase):

    def _Queue(self, *args, **kwargs):
        return self.connection.SimpleQueue(*args, **kwargs)

    def test_is_ack(self):
        q = self.Queue('test_is_no_ack')
        assert not q.no_ack

    def test_queue_args(self):
        q = self.connection.SimpleQueue('test_queue_args',
                                        queue_args={'x-queue-mode': 'lazy'})
        assert q.queue.queue_arguments['x-queue-mode'] == 'lazy'


class test_SimpleBuffer(SimpleBase):

    def Queue(self, *args, **kwargs):
        return self.connection.SimpleBuffer(*args, **kwargs)

    def test_is_no_ack(self):
        q = self.Queue('test_is_no_ack')
        assert q.no_ack
