from __future__ import annotations

import os
from uuid import uuid4

import pytest

import kombu
from kombu import Connection, Consumer, Exchange, Producer, Queue


def get_connection():
    return kombu.Connection(
        f"proton://{os.environ.get('RABBITMQ_HOST', 'localhost')}:"
        f"{os.environ.get('RABBITMQ_5672_TCP', '5672')}",
    )


@pytest.fixture()
def connection():
    return get_connection()


@pytest.mark.env("proton")
def test_connection(connection):
    connection.connect()
    try:
        assert connection.connected
    finally:
        connection.close()


@pytest.mark.env("proton")
def test_publish_and_consume(connection):
    queue_name = f"proton-consume-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
            accept=["json"],
        ):
            Producer(connection).publish(
                {"message": "hello"},
                exchange=queue.exchange,
                routing_key=queue.routing_key,
                serializer="json",
            )

            connection.drain_events(timeout=5)

    assert received == [{"message": "hello"}]


@pytest.mark.env("proton")
def test_publish_multiple_messages(connection):
    queue_name = f"proton-multiple-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        producer = Producer(connection)

        for index in range(5):
            producer.publish(
                {"index": index},
                exchange=queue.exchange,
                routing_key=queue.routing_key,
                serializer="json",
            )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
            accept=["json"],
            prefetch_count=5,
        ):
            for _ in range(5):
                connection.drain_events(timeout=5)

    assert received == [
        {"index": 0},
        {"index": 1},
        {"index": 2},
        {"index": 3},
        {"index": 4},
    ]


@pytest.mark.env("proton")
def test_consumer_ack(connection):
    queue_name = f"proton-ack-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        Producer(connection).publish(
            "ack-me",
            exchange=queue.exchange,
            routing_key=queue.routing_key,
        )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
        ):
            connection.drain_events(timeout=5)

    assert received == ["ack-me"]


@pytest.mark.env("proton")
def test_consumer_reject(connection):
    queue_name = f"proton-reject-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.reject(requeue=False)

    with connection:
        queue.bind(connection).declare()

        Producer(connection).publish(
            "reject-me",
            exchange=queue.exchange,
            routing_key=queue.routing_key,
        )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
        ):
            connection.drain_events(timeout=5)

        assert received == ["reject-me"]


@pytest.mark.env("proton")
def test_consumer_reject_requeue(connection):
    queue_name = f"proton-requeue-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []
    attempts = 0

    def callback(body, message):
        nonlocal attempts

        attempts += 1
        received.append(body)

        if attempts == 1:
            message.reject(requeue=True)
        else:
            message.ack()

    with connection:
        queue.bind(connection).declare()

        Producer(connection).publish(
            "retry-me",
            exchange=queue.exchange,
            routing_key=queue.routing_key,
        )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
        ):
            connection.drain_events(timeout=5)
            connection.drain_events(timeout=5)

    assert received == [
        "retry-me",
        "retry-me",
    ]


@pytest.mark.env("proton")
def test_message_headers(connection):
    queue_name = f"proton-headers-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(
            (
                body,
                message.headers,
            )
        )
        message.ack()

    with connection:
        queue.bind(connection).declare()

        Producer(connection).publish(
            {"hello": "world"},
            exchange=queue.exchange,
            routing_key=queue.routing_key,
            serializer="json",
            headers={
                "x-test": "proton",
                "x-number": 42,
            },
        )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
            accept=["json"],
        ):
            connection.drain_events(timeout=5)

    assert received == [
        (
            {"hello": "world"},
            {
                "x-test": "proton",
                "x-number": 42,
            },
        )
    ]


@pytest.mark.env("proton")
def test_message_content_properties(connection):
    queue_name = f"proton-content-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(message)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        Producer(connection).publish(
            {"hello": "world"},
            exchange=queue.exchange,
            routing_key=queue.routing_key,
            serializer="json",
        )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
            accept=["json"],
        ):
            connection.drain_events(timeout=5)

    assert received[0].content_type == "application/json"
    assert received[0].content_encoding == "utf-8"


@pytest.mark.env("proton")
def test_basic_get_empty_queue(connection):
    queue_name = f"proton-empty-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    with connection:
        queue.bind(connection).declare()

        result = connection.default_channel.basic_get(
            queue_name,
            no_ack=False,
        )

        assert result is None


@pytest.mark.env("proton")
def test_consumer_prefetch(connection):
    queue_name = f"proton-prefetch-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        producer = Producer(connection)

        for index in range(3):
            producer.publish(
                {"index": index},
                exchange=queue.exchange,
                routing_key=queue.routing_key,
                serializer="json",
            )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
            accept=["json"],
            prefetch_count=1,
        ):
            connection.drain_events(timeout=5)
            connection.drain_events(timeout=5)
            connection.drain_events(timeout=5)

    assert len(received) == 3


@pytest.mark.env("proton")
def test_bytes_message(connection):
    queue_name = f"proton-bytes-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        Producer(connection).publish(
            b"hello proton",
            exchange=queue.exchange,
            routing_key=queue.routing_key,
        )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
        ):
            connection.drain_events(timeout=5)

    assert received == [b"hello proton"]


@pytest.mark.env("proton")
def test_unicode_message(connection):
    queue_name = f"proton-unicode-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        Producer(connection).publish(
            "হ্যালো Proton 🚀",
            exchange=queue.exchange,
            routing_key=queue.routing_key,
        )

        with Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
        ):
            connection.drain_events(timeout=5)

    assert received == ["হ্যালো Proton 🚀"]


@pytest.mark.env("proton")
def test_queue_declare_idempotent(connection):
    queue_name = f"proton-declare-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    with connection:
        queue.bind(connection).declare()
        queue.bind(connection).declare()


@pytest.mark.env("proton")
def test_consumer_cancel(connection):
    queue_name = f"proton-cancel-{uuid4().hex}"
    queue = Queue(queue_name, durable=False)

    received = []

    def callback(body, message):
        received.append(body)
        message.ack()

    with connection:
        queue.bind(connection).declare()

        consumer = Consumer(
            connection,
            queues=[queue],
            callbacks=[callback],
        )

        consumer.consume()
        consumer.cancel()

        Producer(connection).publish(
            "should-not-arrive",
            exchange=queue.exchange,
            routing_key=queue.routing_key,
        )

        with pytest.raises(Exception):
            connection.drain_events(timeout=1)

        consumer.close()

    assert received == []
