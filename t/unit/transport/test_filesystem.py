from __future__ import annotations

import tempfile
from fcntl import LOCK_EX, LOCK_SH
from queue import Empty
from unittest.mock import call, patch

import pytest

import t.skip
from kombu import Connection, Consumer, Exchange, Producer, Queue


@t.skip.if_win32
class test_FilesystemTransport:

    def setup(self):
        self.channels = set()
        try:
            data_folder_in = tempfile.mkdtemp()
            data_folder_out = tempfile.mkdtemp()
        except Exception:
            pytest.skip('filesystem transport: cannot create tempfiles')
        self.c = Connection(transport='filesystem',
                            transport_options={
                                'data_folder_in': data_folder_in,
                                'data_folder_out': data_folder_out,
                            })
        self.channels.add(self.c.default_channel)
        self.p = Connection(transport='filesystem',
                            transport_options={
                                'data_folder_in': data_folder_out,
                                'data_folder_out': data_folder_in,
                            })
        self.channels.add(self.p.default_channel)
        self.e = Exchange('test_transport_filesystem')
        self.q = Queue('test_transport_filesystem',
                       exchange=self.e,
                       routing_key='test_transport_filesystem')
        self.q2 = Queue('test_transport_filesystem2',
                        exchange=self.e,
                        routing_key='test_transport_filesystem2')

    def teardown(self):
        # make sure we don't attempt to restore messages at shutdown.
        for channel in self.channels:
            try:
                channel._qos._dirty.clear()
            except AttributeError:
                pass
            try:
                channel._qos._delivered.clear()
            except AttributeError:
                pass

    def _add_channel(self, channel):
        self.channels.add(channel)
        return channel

    def test_produce_consume_noack(self):
        producer = Producer(self._add_channel(self.p.channel()), self.e)
        consumer = Consumer(self._add_channel(self.c.channel()), self.q,
                            no_ack=True)

        for i in range(10):
            producer.publish({'foo': i},
                             routing_key='test_transport_filesystem')

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

    def test_produce_consume(self):
        producer_channel = self._add_channel(self.p.channel())
        consumer_channel = self._add_channel(self.c.channel())
        producer = Producer(producer_channel, self.e)
        consumer1 = Consumer(consumer_channel, self.q)
        consumer2 = Consumer(consumer_channel, self.q2)
        self.q2(consumer_channel).declare()

        for i in range(10):
            producer.publish({'foo': i},
                             routing_key='test_transport_filesystem')
        for i in range(10):
            producer.publish({'foo': i},
                             routing_key='test_transport_filesystem2')

        _received1 = []
        _received2 = []

        def callback1(message_data, message):
            _received1.append(message)
            message.ack()

        def callback2(message_data, message):
            _received2.append(message)
            message.ack()

        consumer1.register_callback(callback1)
        consumer2.register_callback(callback2)

        consumer1.consume()
        consumer2.consume()

        while 1:
            if len(_received1) + len(_received2) == 20:
                break
            self.c.drain_events()

        assert len(_received1) + len(_received2) == 20

        # compression
        producer.publish({'compressed': True},
                         routing_key='test_transport_filesystem',
                         compression='zlib')
        m = self.q(consumer_channel).get()
        assert m.payload == {'compressed': True}

        # queue.delete
        for i in range(10):
            producer.publish({'foo': i},
                             routing_key='test_transport_filesystem')
        assert self.q(consumer_channel).get()
        self.q(consumer_channel).delete()
        self.q(consumer_channel).declare()
        assert self.q(consumer_channel).get() is None

        # queue.purge
        for i in range(10):
            producer.publish({'foo': i},
                             routing_key='test_transport_filesystem2')
        assert self.q2(consumer_channel).get()
        self.q2(consumer_channel).purge()
        assert self.q2(consumer_channel).get() is None


@t.skip.if_win32
class test_FilesystemFanout:
    def setup(self):
        try:
            data_folder_in = tempfile.mkdtemp()
            data_folder_out = tempfile.mkdtemp()
            control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip("filesystem transport: cannot create tempfiles")

        self.consumer_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": data_folder_in,
                "data_folder_out": data_folder_out,
                "control_folder": control_folder,
            },
        )
        self.consume_channel = self.consumer_connection.channel()
        self.produce_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": data_folder_out,
                "data_folder_out": data_folder_in,
                "control_folder": control_folder,
            },
        )
        self.producer_channel = self.produce_connection.channel()
        self.exchange = Exchange("filesystem_exchange_fanout", type="fanout")
        self.q1 = Queue("queue1", exchange=self.exchange)
        self.q2 = Queue("queue2", exchange=self.exchange)

    def teardown(self):
        # make sure we don't attempt to restore messages at shutdown.
        for channel in [self.producer_channel, self.consumer_connection]:
            try:
                channel._qos._dirty.clear()
            except AttributeError:
                pass
            try:
                channel._qos._delivered.clear()
            except AttributeError:
                pass

    def test_produce_consume(self):

        producer = Producer(self.producer_channel, self.exchange)
        consumer1 = Consumer(self.consume_channel, self.q1)
        consumer2 = Consumer(self.consume_channel, self.q2)
        self.q2(self.consume_channel).declare()

        for i in range(10):
            producer.publish({"foo": i})

        _received1 = []
        _received2 = []

        def callback1(message_data, message):
            _received1.append(message)
            message.ack()

        def callback2(message_data, message):
            _received2.append(message)
            message.ack()

        consumer1.register_callback(callback1)
        consumer2.register_callback(callback2)

        consumer1.consume()
        consumer2.consume()

        while 1:
            try:
                self.consume_channel.drain_events()
            except Empty:
                break

        assert len(_received1) + len(_received2) == 20

        # queue.delete
        for i in range(10):
            producer.publish({"foo": i})
        assert self.q1(self.consume_channel).get()
        self.q1(self.consume_channel).delete()
        self.q1(self.consume_channel).declare()
        assert self.q1(self.consume_channel).get() is None

        # queue.purge
        assert self.q2(self.consume_channel).get()
        self.q2(self.consume_channel).purge()
        assert self.q2(self.consume_channel).get() is None


@t.skip.if_win32
class test_FilesystemLock:
    def setup(self):
        try:
            data_folder_in = tempfile.mkdtemp()
            data_folder_out = tempfile.mkdtemp()
            control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip("filesystem transport: cannot create tempfiles")

        self.consumer_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": data_folder_in,
                "data_folder_out": data_folder_out,
                "control_folder": control_folder,
            },
        )
        self.consume_channel = self.consumer_connection.channel()
        self.produce_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": data_folder_out,
                "data_folder_out": data_folder_in,
                "control_folder": control_folder,
            },
        )
        self.producer_channel = self.produce_connection.channel()
        self.exchange = Exchange("filesystem_exchange_lock", type="fanout")
        self.q = Queue("queue1", exchange=self.exchange)

    def teardown(self):
        # make sure we don't attempt to restore messages at shutdown.
        for channel in [self.producer_channel, self.consumer_connection]:
            try:
                channel._qos._dirty.clear()
            except AttributeError:
                pass
            try:
                channel._qos._delivered.clear()
            except AttributeError:
                pass

    def test_lock_during_process(self):
        producer = Producer(self.producer_channel, self.exchange)

        with patch("kombu.transport.filesystem.lock") as lock_m, patch(
            "kombu.transport.filesystem.unlock"
        ) as unlock_m:
            Consumer(self.consume_channel, self.q)
            assert unlock_m.call_count == 1
            lock_m.assert_called_once_with(unlock_m.call_args[0][0], LOCK_EX)

        self.q(self.consume_channel).declare()
        with patch("kombu.transport.filesystem.lock") as lock_m, patch(
            "kombu.transport.filesystem.unlock"
        ) as unlock_m:
            producer.publish({"foo": 1})
            assert unlock_m.call_count == 2
            assert lock_m.call_count == 2
            exchange_file_obj = unlock_m.call_args_list[0][0][0]
            msg_file_obj = unlock_m.call_args_list[1][0][0]
            assert lock_m.call_args_list == [call(exchange_file_obj, LOCK_SH),
                                             call(msg_file_obj, LOCK_EX)]
