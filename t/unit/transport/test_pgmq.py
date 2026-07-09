from __future__ import annotations

import socket
from array import array
from queue import Empty
from unittest.mock import Mock, patch

import pytest

from kombu.exceptions import OperationalError
from kombu.transport.pgmq import (FANOUT_TOPIC_PREFIX, PGMQ_MAX_MESSAGES,
                                  Channel, Transport, _NotifyWaiter)

pytest.importorskip("pgmq")


class test_PGMQ:
    def setup_method(self):
        self.kombu_connection = Mock()
        self.kombu_connection.hostname = "localhost"
        self.kombu_connection.port = 5432
        self.kombu_connection.userid = "postgres"
        self.kombu_connection.password = "postgres"
        self.kombu_connection.virtual_host = "postgres"
        self.kombu_connection.transport_options = {}

        self.transport = Transport(self.kombu_connection)
        self.transport._pgmq_client = Mock()

        self.connection = Mock()
        self.connection.client = self.kombu_connection
        self.connection._get_pgmq_client = self.transport._get_pgmq_client
        self.connection._deliver = Mock()
        self.connection._used_channel_ids = array("H")
        self.connection.channel_max = 65535

        self.channel = Channel(connection=self.connection)

    def test_entity_name(self):
        assert self.channel.entity_name("celery") == "celery"
        assert self.channel.entity_name("my.queue") == "my_queue"

    def test_new_queue(self):
        self.channel._new_queue("celery")
        self.transport._pgmq_client.create_queue.assert_called_once_with("celery")

    def test_new_queue_partitioned(self):
        self.channel._new_queue(
            "celery",
            arguments={
                "x-pgmq-partitioned": True,
                "x-pgmq-partition-interval": 100,
                "x-pgmq-retention-interval": 1000,
            },
        )
        self.transport._pgmq_client.create_partitioned_queue.assert_called_once_with(
            "celery", 100, 1000
        )
        self.transport._pgmq_client.create_queue.assert_not_called()

    def test_new_queue_fifo_index(self):
        self.channel._new_queue("celery", arguments={"x-pgmq-fifo-index": True})
        self.transport._pgmq_client.create_fifo_index.assert_called_once_with("celery")

    def test_new_queue_enables_notify(self):
        self.kombu_connection.transport_options = {
            "use_notify": True,
            "notify_throttle_interval_ms": 0,
        }
        channel = Channel(connection=self.transport)
        with patch.object(self.transport, "_register_notify_queue") as register:
            channel._new_queue("celery")
        self.transport._pgmq_client.enable_notify.assert_called_once_with(
            "celery", throttle_interval_ms=0
        )
        register.assert_called_once_with("celery")

    def test_put(self):
        message = {"body": "hello", "properties": {}}
        self.channel._put("celery", message)
        self.transport._pgmq_client.send.assert_called_once_with("celery", message)

    def test_put_with_delay_and_headers(self):
        message = {
            "body": "hello",
            "headers": {"trace": "abc"},
            "properties": {
                "expiration": "2500",
                "MessageGroupId": "group-1",
            },
        }
        self.channel._put("celery", message)
        self.transport._pgmq_client.send.assert_called_once_with(
            "celery",
            message,
            headers={"trace": "abc", "MessageGroupId": "group-1"},
            delay=2,
        )

    def test_put_with_delay_seconds(self):
        message = {
            "body": "hello",
            "properties": {"DelaySeconds": 5},
        }
        self.channel._put("celery", message)
        self.transport._pgmq_client.send.assert_called_once_with(
            "celery", message, delay=5
        )

    def test_put_topic(self):
        message = {"body": "hello", "properties": {}}
        self.channel._put_topic("orders", message, "orders.created")
        self.transport._pgmq_client.send_topic.assert_called_once_with(
            "orders.orders.created", message
        )

    def test_topic_exchange_deliver_uses_send_topic(self):
        message = {"body": "hello", "properties": {}}
        exchange_type = self.channel.exchange_types["topic"]
        exchange_type.deliver(message, "test_topic", "orders.created")
        self.transport._pgmq_client.send_topic.assert_called_once_with(
            "test_topic.orders.created", message
        )
        self.transport._pgmq_client.send.assert_not_called()

    def test_queue_bind_fanout(self):
        exchange_type = Mock(type="fanout")
        with patch.object(self.channel, "typeof", return_value=exchange_type):
            self.channel._queue_bind("test_fanout", "", None, "workers")
        self.transport._pgmq_client.create_queue.assert_called_with("workers")
        self.transport._pgmq_client.bind_topic.assert_called_once_with(
            f"{FANOUT_TOPIC_PREFIX}.test_fanout", "workers"
        )

    def test_queue_bind_topic(self):
        exchange_type = Mock(type="topic")
        with patch.object(self.channel, "typeof", return_value=exchange_type):
            self.channel._queue_bind("test_topic", "orders.*", None, "orders_q")
        self.transport._pgmq_client.bind_topic.assert_called_once_with(
            "test_topic.orders.*", "orders_q"
        )

    def test_put_fanout(self):
        message = {"body": "hello", "properties": {}}
        self.channel._put_fanout("events", message, "")
        self.transport._pgmq_client.send_topic.assert_called_once_with(
            f"{FANOUT_TOPIC_PREFIX}.events", message
        )

    def test_get_fifo_grouped(self):
        self.kombu_connection.transport_options = {
            "fifo_mode": "grouped",
            "wait_time_seconds": 0,
        }
        channel = Channel(connection=self.connection)

        payload = {
            "body": "hello",
            "properties": {"delivery_info": {}},
        }
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.read_grouped.return_value = [pgmq_message]

        result = channel._get("celery")

        assert result is payload
        self.transport._pgmq_client.read_grouped.assert_called_once_with(
            "celery", vt=1800, qty=1
        )
        self.transport._pgmq_client.read.assert_not_called()

    def test_get_with_ack(self):
        payload = {
            "body": "hello",
            "properties": {"delivery_info": {}},
        }
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.read_with_poll.return_value = [pgmq_message]

        result = self.channel._get("celery")

        assert result is payload
        assert result["properties"]["delivery_info"]["pgmq_queue"] == "celery"
        assert result["properties"]["delivery_info"]["pgmq_msg_id"] == 42
        assert result["properties"]["delivery_info"]["pgmq_message"] == {
            "msg_id": 42,
            "read_ct": 1,
        }
        self.transport._pgmq_client.read_with_poll.assert_called_once_with(
            "celery",
            vt=1800,
            qty=1,
            max_poll_seconds=10,
            poll_interval_ms=100,
        )

    def test_get_no_ack(self):
        self.channel._noack_queues.add("celery")

        payload = {
            "body": "hello",
            "properties": {"delivery_info": {}},
        }
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.pop.return_value = pgmq_message

        result = self.channel._get("celery")

        assert result is payload
        assert "pgmq_msg_id" not in result["properties"]["delivery_info"]
        self.transport._pgmq_client.pop.assert_called_once_with("celery", qty=1)

    def test_get_empty(self):
        self.transport._pgmq_client.read_with_poll.return_value = []
        with pytest.raises(Empty):
            self.channel._get("celery")

    def test_get_bulk_delivers_prefetch_messages(self):
        self.channel.qos.prefetch_count = 2
        payloads = [
            {"body": "one", "properties": {"delivery_info": {}}},
            {"body": "two", "properties": {"delivery_info": {}}},
        ]
        messages = [
            Mock(msg_id=1, read_ct=1, message=payloads[0]),
            Mock(msg_id=2, read_ct=1, message=payloads[1]),
        ]
        self.transport._pgmq_client.read_with_poll.return_value = messages

        self.channel._get_bulk("celery")

        self.transport._pgmq_client.read_with_poll.assert_called_once_with(
            "celery",
            vt=1800,
            qty=2,
            max_poll_seconds=10,
            poll_interval_ms=100,
        )
        assert self.connection._deliver.call_count == 2

    def test_get_bulk_respects_max_messages(self):
        self.channel.qos.prefetch_count = 100
        self.transport._pgmq_client.read_with_poll.return_value = []

        with pytest.raises(Empty):
            self.channel._get_bulk("celery")

        self.transport._pgmq_client.read_with_poll.assert_called_once_with(
            "celery",
            vt=1800,
            qty=PGMQ_MAX_MESSAGES,
            max_poll_seconds=10,
            poll_interval_ms=100,
        )

    def test_reset_cycle_uses_get_bulk(self):
        self.channel._active_queues = ["celery"]
        self.channel._reset_cycle()
        assert self.channel._cycle.fun == self.channel._get_bulk

    def test_basic_ack(self):
        message = Mock()
        message.delivery_info = {
            "pgmq_queue": "celery",
            "pgmq_msg_id": 7,
        }
        self.channel.qos._delivered["tag-1"] = message

        self.channel.basic_ack("tag-1")

        self.transport._pgmq_client.delete.assert_called_once_with("celery", 7)
        assert "tag-1" in self.channel.qos._dirty

    def test_basic_reject_requeue(self):
        message = Mock()
        message.delivery_info = {
            "pgmq_queue": "celery",
            "pgmq_msg_id": 7,
        }
        self.channel.qos._delivered["tag-1"] = message

        self.channel.basic_reject("tag-1", requeue=True)

        self.transport._pgmq_client.set_vt.assert_called_once_with("celery", 7, 0)
        self.transport._pgmq_client.delete.assert_not_called()
        assert "tag-1" in self.channel.qos._dirty

    def test_basic_reject_no_requeue(self):
        message = Mock()
        message.delivery_info = {
            "pgmq_queue": "celery",
            "pgmq_msg_id": 7,
        }
        self.channel.qos._delivered["tag-1"] = message

        self.channel.basic_reject("tag-1", requeue=False)

        self.transport._pgmq_client.delete.assert_called_once_with("celery", 7)
        self.transport._pgmq_client.set_vt.assert_not_called()

    def test_purge_and_size(self):
        self.transport._pgmq_client.purge.return_value = 3
        self.transport._pgmq_client.metrics.return_value = Mock(queue_length=5)

        assert self.channel._purge("celery") == 3
        assert self.channel._size("celery") == 5

    def test_delete_queue(self):
        self.channel._delete("celery")
        self.transport._pgmq_client.drop_queue.assert_called_once_with("celery")

    def test_has_queue(self):
        record = Mock(queue_name="celery")
        self.transport._pgmq_client.list_queues.return_value = [record]

        assert self.channel._has_queue("celery") is True
        assert self.channel._has_queue("missing") is False

    def test_wait_time_seconds_legacy_alias(self):
        self.kombu_connection.transport_options = {"max_poll_seconds": 5}
        channel = Channel(connection=self.connection)
        assert channel.wait_time_seconds == 5

    def test_url_connection(self):
        with patch("kombu.transport.pgmq.PGMQueue") as PGMQueueMock:
            conn = Mock()
            conn.hostname = "db.example.com"
            conn.port = 5433
            conn.userid = "user"
            conn.password = "secret"
            conn.virtual_host = "/mydb"
            conn.transport_options = {}

            transport = Transport(conn)
            transport._get_pgmq_client()

            PGMQueueMock.assert_called_once_with(
                host="db.example.com",
                port="5433",
                database="mydb",
                username="user",
                password="secret",
                vt=1800,
                init_extension=True,
                pool_size=10,
            )

    def test_close_connection_closes_pool(self):
        pool = Mock()
        self.transport._pgmq_client = Mock(pool=pool)

        self.transport.close_connection(Mock())

        pool.close.assert_called_once()
        assert self.transport._pgmq_client is None

    def test_driver_version(self):
        with patch("pgmq.__version__", "1.2.3"):
            assert Transport(self.kombu_connection).driver_version() == "1.2.3"

    def test_drain_events_uses_notify_waiter(self):
        self.kombu_connection.transport_options = {
            "use_notify": True,
            "polling_interval": 0.5,
        }
        transport = Transport(self.kombu_connection)
        transport._pgmq_client = self.transport._pgmq_client
        transport._pgmq_client.config.dsn = "postgresql://localhost/test"
        waiter = Mock()
        waiter.wait.return_value = False
        transport._notify_waiter = waiter

        with patch.object(transport.cycle, "get", side_effect=Empty()):
            with pytest.raises(socket.timeout):
                transport.drain_events(Mock(), timeout=0.01)

        waiter.wait.assert_called()


class test_NotifyWaiter:
    def test_register_and_wait(self):
        mock_conn = Mock()
        mock_conn.notifies.return_value = iter([Mock()])
        with patch("kombu.transport.pgmq.psycopg.connect", return_value=mock_conn):
            waiter = _NotifyWaiter("postgresql://localhost/test")
            waiter.register("celery")
            assert waiter.wait(1.0) is True
            mock_conn.execute.assert_called_once_with('LISTEN "pgmq.q_celery.INSERT";')

    def test_register_duplicate_channel(self):
        mock_conn = Mock()
        with patch("kombu.transport.pgmq.psycopg.connect", return_value=mock_conn):
            waiter = _NotifyWaiter("postgresql://localhost/test")
            waiter.register("celery")
            waiter.register("celery")
            mock_conn.execute.assert_called_once()

    def test_wait_without_channels(self):
        waiter = _NotifyWaiter("postgresql://localhost/test")
        assert waiter.wait(1.0) is False

    def test_wait_returns_false_when_no_notification(self):
        mock_conn = Mock()
        mock_conn.notifies.return_value = iter([])
        with patch("kombu.transport.pgmq.psycopg.connect", return_value=mock_conn):
            waiter = _NotifyWaiter("postgresql://localhost/test")
            waiter.register("celery")
            assert waiter.wait(1.0) is False

    def test_close(self):
        mock_conn = Mock()
        with patch("kombu.transport.pgmq.psycopg.connect", return_value=mock_conn):
            waiter = _NotifyWaiter("postgresql://localhost/test")
            waiter.register("celery")
            waiter.close()
            mock_conn.close.assert_called_once()
            assert waiter._channels == set()


class test_PGMQ_additional:
    def setup_method(self):
        self.kombu_connection = Mock()
        self.kombu_connection.hostname = "localhost"
        self.kombu_connection.port = 5432
        self.kombu_connection.userid = "postgres"
        self.kombu_connection.password = "postgres"
        self.kombu_connection.virtual_host = "postgres"
        self.kombu_connection.transport_options = {}

        self.transport = Transport(self.kombu_connection)
        self.transport._pgmq_client = Mock()

        self.connection = Mock()
        self.connection.client = self.kombu_connection
        self.connection._get_pgmq_client = self.transport._get_pgmq_client
        self.connection._deliver = Mock()
        self.connection._used_channel_ids = array("H")
        self.connection.channel_max = 65535
        self.connection._callbacks = {}

        self.channel = Channel(connection=self.connection)

    def test_channel_import_error_without_pgmq(self):
        with patch("kombu.transport.pgmq.PGMQueue", None):
            with pytest.raises(ImportError, match="pip install kombu\\[pgmq\\]"):
                Channel(connection=self.connection)

    def test_transport_import_error_without_pgmq(self):
        with patch("kombu.transport.pgmq.PGMQueue", None):
            with pytest.raises(ImportError, match="pip install kombu\\[pgmq\\]"):
                Transport(self.kombu_connection)

    def test_basic_consume_no_ack(self):
        self.channel.basic_consume("celery", True, Mock(), "tag-1")
        assert "celery" in self.channel._noack_queues

    def test_basic_consume_enables_notify(self):
        self.kombu_connection.transport_options = {"use_notify": True}
        channel = Channel(connection=self.connection)
        with patch.object(channel, "_maybe_enable_notify") as enable:
            channel.basic_consume("celery", False, Mock(), "tag-1")
        enable.assert_called_once_with("celery")

    def test_basic_cancel_removes_no_ack_queue(self):
        self.channel._noack_queues.add("celery")
        self.channel._consumers.add("tag-1")
        self.channel._tag_to_queue["tag-1"] = "celery"
        self.channel._active_queues.append("celery")
        self.channel.basic_cancel("tag-1")
        assert "celery" not in self.channel._noack_queues

    def test_channel_drain_events_raises_empty_without_consumers(self):
        with pytest.raises(Empty):
            self.channel.drain_events()

    def test_put_with_invalid_expiration(self):
        message = {"body": "hello", "properties": {"expiration": "not-a-number"}}
        self.channel._put("celery", message)
        self.transport._pgmq_client.send.assert_called_once_with("celery", message)

    def test_put_with_zero_expiration(self):
        message = {"body": "hello", "properties": {"expiration": "0"}}
        self.channel._put("celery", message)
        self.transport._pgmq_client.send.assert_called_once_with("celery", message)

    def test_normalize_messages_none(self):
        assert self.channel._normalize_messages(None) == []

    def test_queue_bind_topic_empty_routing_key(self):
        exchange_type = Mock(type="topic")
        with patch.object(self.channel, "typeof", return_value=exchange_type):
            self.channel._queue_bind("test_topic", "", None, "orders_q")
        self.transport._pgmq_client.bind_topic.assert_not_called()

    def test_get_without_poll(self):
        self.kombu_connection.transport_options = {"wait_time_seconds": 0}
        channel = Channel(connection=self.connection)
        payload = {"body": "hello", "properties": {"delivery_info": {}}}
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.read.return_value = pgmq_message

        channel._get("celery")

        self.transport._pgmq_client.read.assert_called_once_with(
            "celery", vt=1800, qty=1
        )
        self.transport._pgmq_client.read_with_poll.assert_not_called()

    def test_get_fifo_grouped_with_poll(self):
        self.kombu_connection.transport_options = {"fifo_mode": "grouped"}
        channel = Channel(connection=self.connection)
        payload = {"body": "hello", "properties": {"delivery_info": {}}}
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.read_grouped_with_poll.return_value = [
            pgmq_message,
        ]

        channel._get("celery")

        self.transport._pgmq_client.read_grouped_with_poll.assert_called_once_with(
            "celery",
            vt=1800,
            qty=1,
            max_poll_seconds=10,
            poll_interval_ms=100,
        )

    def test_get_fifo_round_robin(self):
        self.kombu_connection.transport_options = {
            "fifo_mode": "round_robin",
            "wait_time_seconds": 0,
        }
        channel = Channel(connection=self.connection)
        payload = {"body": "hello", "properties": {"delivery_info": {}}}
        pgmq_message = Mock(msg_id=42, read_ct=1, message=payload)
        self.transport._pgmq_client.read_grouped_rr.return_value = [
            pgmq_message,
        ]

        channel._get("celery")

        self.transport._pgmq_client.read_grouped_rr.assert_called_once_with(
            "celery", vt=1800, qty=1
        )

    def test_get_bulk_zero_estimate(self):
        with patch.object(self.channel, "_get_message_estimate", return_value=0):
            with pytest.raises(Empty):
                self.channel._get_bulk("celery")

    def test_basic_ack_multiple_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self.channel.basic_ack("tag-1", multiple=True)

    def test_basic_ack_missing_delivery_tag(self):
        self.channel.basic_ack("missing-tag")
        self.transport._pgmq_client.delete.assert_not_called()

    def test_basic_ack_without_pgmq_delivery_info(self):
        message = Mock()
        message.delivery_info = {}
        self.channel.qos._delivered["tag-1"] = message
        self.channel.basic_ack("tag-1")
        self.transport._pgmq_client.delete.assert_not_called()

    def test_basic_reject_keyerror_fallback(self):
        with patch.object(self.channel.qos, "get", side_effect=KeyError):
            with patch("kombu.transport.virtual.base.Channel.basic_reject") as reject:
                self.channel.basic_reject("tag-1", requeue=True)
        reject.assert_called_once_with("tag-1", requeue=True)

    def test_basic_reject_without_pgmq_delivery_info(self):
        message = Mock()
        message.delivery_info = {}
        self.channel.qos._delivered["tag-1"] = message
        self.channel.basic_reject("tag-1", requeue=True)
        self.transport._pgmq_client.set_vt.assert_not_called()

    def test_restore_strips_pgmq_delivery_info(self):
        message = Mock()
        message.delivery_info = {
            "exchange": "",
            "routing_key": "celery",
            "pgmq_queue": "celery",
            "pgmq_msg_id": 1,
            "pgmq_message": {"msg_id": 1},
        }
        message.serializable.return_value = {
            "body": "hello",
            "properties": {"delivery_info": message.delivery_info.copy()},
        }
        with patch.object(self.channel, "_lookup", return_value=["celery"]):
            with patch.object(self.channel, "_put") as put:
                self.channel._restore(message)
        assert "pgmq_queue" not in message.delivery_info
        assert "pgmq_msg_id" not in message.delivery_info
        assert "pgmq_message" not in message.delivery_info
        put.assert_called_once()

    def test_conninfo(self):
        assert self.channel.conninfo is self.kombu_connection

    def test_queue_name_prefix(self):
        self.kombu_connection.transport_options = {
            "queue_name_prefix": "kombu.",
        }
        channel = Channel(connection=self.connection)
        assert channel.canonical_queue_name("celery") == "kombu_celery"

    def test_init_extension_and_pool_size(self):
        self.kombu_connection.transport_options = {
            "init_extension": False,
            "pool_size": 5,
        }
        channel = Channel(connection=self.connection)
        assert channel.init_extension is False
        assert channel.pool_size == 5

    def test_invalid_fifo_mode(self):
        self.kombu_connection.transport_options = {"fifo_mode": "invalid"}
        with pytest.raises(ValueError, match="Invalid fifo_mode"):
            Channel(connection=self.connection).fifo_mode

    def test_new_queue_fifo_mode_transport_option(self):
        self.kombu_connection.transport_options = {"fifo_mode": "grouped"}
        channel = Channel(connection=self.connection)
        channel._new_queue("celery")
        self.transport._pgmq_client.create_fifo_index.assert_called_once_with("celery")

    def test_register_notify_queue_disabled(self):
        transport = Transport(self.kombu_connection)
        transport._register_notify_queue("celery")

    def test_register_notify_queue_enabled(self):
        self.kombu_connection.transport_options = {"use_notify": True}
        transport = Transport(self.kombu_connection)
        transport._pgmq_client = Mock(config=Mock(dsn="postgresql://localhost/test"))
        with patch.object(transport, "_get_notify_waiter") as get_waiter:
            waiter = Mock()
            get_waiter.return_value = waiter
            transport._register_notify_queue("celery")
        waiter.register.assert_called_once_with("celery")

    def test_get_notify_waiter(self):
        transport = Transport(self.kombu_connection)
        transport._pgmq_client = Mock(config=Mock(dsn="postgresql://localhost/test"))
        with patch("kombu.transport.pgmq._NotifyWaiter") as waiter_cls:
            transport._get_notify_waiter()
        waiter_cls.assert_called_once_with("postgresql://localhost/test")

    def test_drain_events_sleeps_on_empty_poll(self):
        self.kombu_connection.transport_options = {"polling_interval": 0.5}
        transport = Transport(self.kombu_connection)
        with patch.object(transport.cycle, "get", side_effect=Empty()):
            with patch("kombu.transport.pgmq.sleep") as sleep:
                with pytest.raises(socket.timeout):
                    transport.drain_events(Mock(), timeout=0.01)
        sleep.assert_called()

    def test_drain_events_caps_polling_interval(self):
        self.kombu_connection.transport_options = {"polling_interval": 10}
        transport = Transport(self.kombu_connection)
        with patch.object(transport.cycle, "get", side_effect=Empty()):
            with pytest.raises(socket.timeout):
                transport.drain_events(Mock(), timeout=0.01)

    def test_drain_events_delivers_message(self):
        transport = Transport(self.kombu_connection)
        with patch.object(transport.cycle, "get") as get:
            transport.drain_events(Mock(), timeout=1)
        get.assert_called_once()

    def test_conn_string_connection(self):
        with patch("kombu.transport.pgmq.PGMQueue") as PGMQueueMock:
            conn = Mock()
            conn.transport_options = {"conn_string": "postgresql://custom/db"}
            transport = Transport(conn)
            transport._get_pgmq_client()
            PGMQueueMock.assert_called_once_with(
                conn_string="postgresql://custom/db",
                vt=1800,
                init_extension=True,
                pool_size=10,
            )

    def test_basic_cancel_unknown_consumer_tag(self):
        self.channel.basic_cancel("unknown-tag")

    def test_channel_drain_events_with_consumer(self):
        self.channel._consumers.add("tag-1")
        self.channel._active_queues.append("celery")
        self.channel._tag_to_queue["tag-1"] = "celery"
        self.channel._reset_cycle()
        with patch.object(self.channel, "_poll") as poll:
            self.channel.drain_events(timeout=1)
        poll.assert_called_once()

    def test_url_connection_empty_database(self):
        with patch("kombu.transport.pgmq.PGMQueue") as PGMQueueMock:
            conn = Mock()
            conn.hostname = "localhost"
            conn.port = 5432
            conn.userid = "user"
            conn.password = "secret"
            conn.virtual_host = ""
            conn.transport_options = {}
            transport = Transport(conn)
            transport._get_pgmq_client()
            PGMQueueMock.assert_called_once_with(
                host="localhost",
                port="5432",
                database="postgres",
                username="user",
                password="secret",
                vt=1800,
                init_extension=True,
                pool_size=10,
            )

    def test_url_connection_default_database(self):
        with patch("kombu.transport.pgmq.PGMQueue") as PGMQueueMock:
            conn = Mock()
            conn.hostname = "localhost"
            conn.port = 5432
            conn.userid = None
            conn.password = None
            conn.virtual_host = "/"
            conn.transport_options = {}
            transport = Transport(conn)
            transport._get_pgmq_client()
            PGMQueueMock.assert_called_once_with(
                host="localhost",
                port="5432",
                database="postgres",
                username="postgres",
                password="",
                vt=1800,
                init_extension=True,
                pool_size=10,
            )

    def test_establish_connection_failure(self):
        transport = Transport(self.kombu_connection)
        with patch.object(transport, "verify_connection", return_value=False):
            with pytest.raises(OperationalError, match="Could not connect"):
                transport.establish_connection()

    def test_establish_connection_success(self):
        transport = Transport(self.kombu_connection)
        with patch.object(transport, "verify_connection", return_value=True):
            with patch.object(
                transport, "create_channel", return_value=Mock()
            ) as create_channel:
                connection = transport.establish_connection()
        create_channel.assert_called_once()
        assert connection is transport

    def test_close_connection_closes_notify_waiter(self):
        transport = Transport(self.kombu_connection)
        waiter = Mock()
        transport._notify_waiter = waiter
        transport._pgmq_client = Mock(pool=None)
        transport.close_connection(Mock())
        waiter.close.assert_called_once()
        assert transport._notify_waiter is None

    def test_verify_connection_success(self):
        transport = Transport(self.kombu_connection)
        transport._pgmq_client = Mock()
        transport._pgmq_client.list_queues.return_value = []
        assert transport.verify_connection(Mock()) is True

    def test_verify_connection_failure(self):
        transport = Transport(self.kombu_connection)
        transport._pgmq_client = Mock()
        transport._pgmq_client.list_queues.side_effect = Exception("boom")
        assert transport.verify_connection(Mock()) is False

    def test_as_uri(self):
        uri = "pgmq://user:secret@localhost:5432/mydb"
        assert Transport.as_uri(uri) == ("pgmq://user:%2A%2A@localhost:5432/mydb")
        assert Transport.as_uri(uri, include_password=True) == uri
        assert Transport.as_uri("not-a-uri") == "not-a-uri"

    def test_driver_version_unknown(self):
        # lazy loading pgmq module
        import pgmq as pgmq_module

        version = getattr(pgmq_module, "__version__", None)
        if hasattr(pgmq_module, "__version__"):
            del pgmq_module.__version__
        try:
            assert Transport(self.kombu_connection).driver_version() == "Unknown"
        finally:
            if version is not None:
                pgmq_module.__version__ = version
