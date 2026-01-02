from __future__ import annotations

import tempfile
import shutil
from queue import Empty
from unittest.mock import call, patch

import pytest

import t.skip
from kombu import Connection, Consumer, Exchange, Producer, Queue
from kombu.transport.filesystem import Channel


@t.skip.if_win32
class test_FilesystemTransport:

    def setup_method(self):
        self.channels = set()
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip('filesystem transport: cannot create tempfiles')
        self.c = Connection(transport='filesystem',
                            transport_options={
                                'data_folder_in': self.data_folder_in,
                                'data_folder_out': self.data_folder_out,
                                'control_folder': self.control_folder,
                            })
        self.channels.add(self.c.default_channel)
        self.p = Connection(transport='filesystem',
                            transport_options={
                                'data_folder_in': self.data_folder_out,
                                'data_folder_out': self.data_folder_in,
                                'control_folder': self.control_folder,
                            })
        self.channels.add(self.p.default_channel)
        self.e_name = 'test_transport_filesystem'
        self.e = Exchange(self.e_name)
        self.q = Queue('test_transport_filesystem',
                       exchange=self.e,
                       routing_key='test_transport_filesystem')
        self.q2 = Queue('test_transport_filesystem2',
                        exchange=self.e,
                        routing_key='test_transport_filesystem2')

    def teardown_method(self):
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

        # clean-up temporary folders
        try:
            shutil.rmtree(self.data_folder_in)
            shutil.rmtree(self.data_folder_out)
            shutil.rmtree(self.control_folder)
        except OSError:
            pass

    def _add_channel(self, channel):
        self.channels.add(channel)
        return channel

    def _prepare_bind(self, channel: Channel, queue: Queue) -> tuple:
        # create exchange_queue_t tuple (see transport/filesystem.py)
        bind: tuple = channel.typeof(self.e_name).prepare_bind(queue.name, self.e_name, queue.routing_key, None)
        return tuple(map(lambda v: v or '', bind))

    def test_produce_consume_noack(self):
        try:
            consumer_channel = self._add_channel(self.c.channel())
            producer = Producer(self._add_channel(self.p.channel()), self.e)
            consumer = Consumer(consumer_channel, self.q,
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
        finally:
            # queue bindings must not leak between test cases
            self.q(consumer_channel).delete()

    def test_produce_consume(self):
        try:
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

            # assert q2 is in consumer_channel's table
            consumer_channel_table: list[tuple] = consumer_channel.get_table(self.e_name)
            assert len(consumer_channel_table) == 2
            assert self._prepare_bind(consumer_channel, self.q) in consumer_channel_table
            assert self._prepare_bind(consumer_channel, self.q2) in consumer_channel_table

            self.q(consumer_channel).delete()

            # assert only q2 is in consumer_channel's table after .delete()
            consumer_channel_table2: list[tuple] = consumer_channel.get_table(self.e_name)
            assert len(consumer_channel_table2) == 1
            assert self._prepare_bind(consumer_channel, self.q) not in consumer_channel_table2
            assert self._prepare_bind(consumer_channel, self.q2) in consumer_channel_table2

            self.q(consumer_channel).declare()
            assert self.q(consumer_channel).get() is None

            # queue.purge
            for i in range(10):
                producer.publish({'foo': i},
                                 routing_key='test_transport_filesystem2')
            assert self.q2(consumer_channel).get()
            self.q2(consumer_channel).purge()
            assert self.q2(consumer_channel).get() is None
        finally:
            # queue bindings must not leak between test cases
            self.q(consumer_channel).delete()
            self.q2(consumer_channel).delete()


@t.skip.if_win32
class test_FilesystemFanout:
    def setup_method(self):
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip("filesystem transport: cannot create tempfiles")

        self.consumer_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_in,
                "data_folder_out": self.data_folder_out,
                "control_folder": self.control_folder,
            },
        )
        self.consume_channel = self.consumer_connection.channel()
        self.produce_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_out,
                "data_folder_out": self.data_folder_in,
                "control_folder": self.control_folder,
            },
        )
        self.producer_channel = self.produce_connection.channel()
        self.exchange = Exchange("filesystem_exchange_fanout", type="fanout")
        self.q1 = Queue("queue1", exchange=self.exchange)
        self.q2 = Queue("queue2", exchange=self.exchange)

    def teardown_method(self):
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

        # clean-up temporary folders
        try:
            shutil.rmtree(self.data_folder_in)
            shutil.rmtree(self.data_folder_out)
            shutil.rmtree(self.control_folder)
        except OSError:
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
    def setup_method(self):
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip("filesystem transport: cannot create tempfiles")

        self.consumer_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_in,
                "data_folder_out": self.data_folder_out,
                "control_folder": self.control_folder,
            },
        )
        self.consume_channel = self.consumer_connection.channel()
        self.produce_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_out,
                "data_folder_out": self.data_folder_in,
                "control_folder": self.control_folder,
            },
        )
        self.producer_channel = self.produce_connection.channel()
        self.exchange = Exchange("filesystem_exchange_lock", type="fanout")
        self.q = Queue("queue1", exchange=self.exchange)

    def teardown_method(self):
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

        # clean-up temporary folders
        try:
            shutil.rmtree(self.data_folder_in)
            shutil.rmtree(self.data_folder_out)
            shutil.rmtree(self.control_folder)
        except OSError:
            pass

    def test_lock_during_process(self):
        pytest.importorskip('fcntl')
        from fcntl import LOCK_EX, LOCK_SH

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
