from __future__ import absolute_import, unicode_literals

import pytest
import socket

from kombu import Connection, Exchange, Queue, Consumer, Producer


class test_PyroTransport:

    def setup(self):
        self.c = Connection(transport='pyro', virtual_host="kombu.broker")
        self.e = Exchange('test_transport_pyro')
        self.q = Queue('test_transport_pyro',
                       exchange=self.e,
                       routing_key='test_transport_pyro')
        self.q2 = Queue('test_transport_pyro2',
                        exchange=self.e,
                        routing_key='test_transport_pyro2')
        self.fanout = Exchange('test_transport_pyro_fanout', type='fanout')
        self.q3 = Queue('test_transport_pyro_fanout1',
                        exchange=self.fanout)
        self.q4 = Queue('test_transport_pyro_fanout2',
                        exchange=self.fanout)

    def test_driver_version(self):
        assert self.c.transport.driver_version()

    @pytest.mark.skip("requires running Pyro nameserver and Kombu Broker")
    def test_produce_consume_noack(self):
        channel = self.c.channel()
        producer = Producer(channel, self.e)
        consumer = Consumer(channel, self.q, no_ack=True)

        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_pyro')

        _received = []

        def callback(message_data, message):
            _received.append(message)

        consumer.register_callback(callback)
        consumer.consume()

        while 1:
            if len(_received) == 10:
                break
            self.c.drain_events()

        assert len(_received) == 10

    def test_drain_events(self):
        with pytest.raises(socket.timeout):
            self.c.drain_events(timeout=0.1)

        c1 = self.c.channel()
        c2 = self.c.channel()

        with pytest.raises(socket.timeout):
            self.c.drain_events(timeout=0.1)

        del(c1)  # so pyflakes doesn't complain.
        del(c2)

    @pytest.mark.skip("requires running Pyro nameserver and Kombu Broker")
    def test_drain_events_unregistered_queue(self):
        c1 = self.c.channel()
        producer = self.c.Producer()
        consumer = self.c.Consumer([self.q2])

        producer.publish(
            {'hello': 'world'},
            declare=consumer.queues,
            routing_key=self.q2.routing_key,
            exchange=self.q2.exchange,
        )
        message = consumer.queues[0].get()._raw

        class Cycle(object):

            def get(self, callback, timeout=None):
                return (message, 'foo'), c1

        self.c.transport.cycle = Cycle()
        self.c.drain_events()

    @pytest.mark.skip("requires running Pyro nameserver and Kombu Broker")
    def test_queue_for(self):
        chan = self.c.channel()
        x = chan._queue_for('foo')
        assert x
        assert chan._queue_for('foo') is x
