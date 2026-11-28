from __future__ import annotations

import os
import select
import socket
from time import monotonic
from unittest.mock import patch
from uuid import uuid4

import pytest

from kombu import Connection, Consumer, Producer, Queue
from kombu.asynchronous.hub import Hub

pytestmark = pytest.mark.env('redis')


@pytest.fixture
def consuming():
    name = f'brpop-qos-{uuid4().hex}'
    connection = Connection(
        f"redis://{os.environ.get('REDIS_HOST', 'localhost')}:"
        f"{os.environ.get('REDIS_6379_TCP', '6379')}",
        transport_options={
            'polling_interval': 1,
            'socket_timeout': 5,
            'unacked_key': f'{name}-unacked',
            'unacked_index_key': f'{name}-unacked-index',
            'unacked_mutex_key': f'{name}-unacked-mutex',
        },
    )
    queue = Queue(name, durable=False)
    received = []
    consumer = Consumer(
        connection, queues=[queue], prefetch_count=2,
        callbacks=[lambda body, message: received.append(message)],
        accept=['json'],
    )
    try:
        consumer.consume()
        Producer(connection).publish(1, routing_key=name, serializer='json')
        connection.drain_events(timeout=5)
        assert [message.payload for message in received] == [1]
        yield connection, queue, consumer, received
    finally:
        for message in received:
            if not message.acknowledged:
                message.ack()
        connection.release()
        with connection.clone() as cleanup:
            queue(cleanup).delete()


def tick(hub):
    for callback in list(hub.on_tick):
        callback()


def run_until(hub, predicate):
    deadline = monotonic() + 5
    while not predicate() and monotonic() < deadline:
        hub.run_once()
    assert predicate()


def pending_reply(consuming, hub=None, message='token'):
    """Leave a reply pending: 'nil', 'token' or 'popped'.

    After a pop that empties the queue, the consumer waits on the wake list.
    Its reply carries no message: the token the first publish left ('nil'),
    or the token of a message published since ('token'), which stays in its
    queue. A 'popped' reply comes from a pop while messages are left: it
    holds a message the script already recorded in unacked.
    """
    connection, queue, consumer, received = consuming
    channel = consumer.channel
    producer = Producer(connection)
    if message == 'token':
        producer.publish(2, routing_key=queue.name, serializer='json')
    elif message == 'popped':
        producer.publish(2, routing_key=queue.name, serializer='json')
        producer.publish(3, routing_key=queue.name, serializer='json')
        connection.drain_events(timeout=5)
        received.pop(0).ack()
    if hub is not None:
        connection.register_with_event_loop(hub)
        tick(hub)
    else:
        connection.transport.cycle.on_poll_start()
    pending = channel._in_poll
    assert pending is not None
    sock = pending._sock
    consumer.qos(prefetch_count=1)
    # Wait without consuming the reply into redis-py's parser. Check its
    # decoded result after resuming, independently of RESP2/RESP3 framing.
    assert select.select([sock], [], [], 5)[0]
    return pending, sock


def assert_paused(connection, consumer, sock, hub=None):
    cycle = connection.transport.cycle
    fd = sock.fileno()
    assert not consumer.channel.qos.can_consume()
    assert fd not in cycle.fds
    if cycle.fds:
        assert fd not in dict(cycle.poller.poll(0))
    assert cycle._fd_to_chan[fd] == (consumer.channel, 'BRPOP')
    assert select.select([sock], [], [], 0)[0]
    if hub is not None:
        assert fd not in hub.readers


@pytest.mark.parametrize('message', ['nil', 'token'])
def test_hub_pauses_pending_brpop_until_ack(consuming, message):
    connection, queue, consumer, received = consuming
    channel = consumer.channel
    hub = Hub()
    try:
        pending, sock = pending_reply(consuming, hub, message)
        with patch.object(channel, '_brpop_start', wraps=channel._brpop_start) as start:
            tick(hub)
            assert_paused(connection, consumer, sock, hub)
            # A token leaves its message in the queue. (Not channel.client:
            # it would read the pending reply.)
            client = channel._create_client()
            assert client.hlen(channel.unacked_key) == 1
            assert client.llen(queue.name) == (message == 'token')
            timers = []
            timer = hub.call_repeatedly(0.01, lambda: timers.append(True))
            try:
                run_until(hub, lambda: bool(timers))
                # on_tick must not put the readable, suspended fd back.
                assert_paused(connection, consumer, sock, hub)
                assert channel._in_poll is pending
                assert [item.payload for item in received] == [1]
                start.assert_not_called()
                received[0].ack()
                # The pending reply is read first, then the pop follows.
                run_until(hub, lambda: len(received) == 2 if message != 'nil'
                          else start.called)
            finally:
                timer.cancel()
            assert start.called
        assert channel.client.connection._sock is sock
        assert sock.fileno() in hub.readers
        assert [item.payload for item in received] == (
            [1] if message == 'nil' else [1, 2])
        if message != 'nil':
            received[1].ack()
        client = channel._create_client()  # The next command is in flight.
        assert client.hlen(channel.unacked_key) == 0
        assert client.zcard(channel.unacked_index_key) == 0
    finally:
        hub.close()


def test_drain_events_resumes_pending_brpop_without_resending(consuming):
    connection, queue, consumer, received = consuming
    channel = consumer.channel
    pending, sock = pending_reply(consuming)
    with patch.object(channel, '_brpop_start', wraps=channel._brpop_start) as start:
        with pytest.raises(socket.timeout):
            connection.drain_events(timeout=0.01)
        assert_paused(connection, consumer, sock)
        assert channel._in_poll is pending
        assert [item.payload for item in received] == [1]
        received[0].ack()
        connection.drain_events(timeout=5)
        # Only the pop that follows the wake-up, once its reply was read.
        start.assert_called_once_with()
    assert channel.client.connection._sock is sock
    assert [item.payload for item in received] == [1, 2]
    received[1].ack()
    assert channel.client.hlen(channel.unacked_key) == 0
    assert channel.client.zcard(channel.unacked_index_key) == 0


@pytest.mark.parametrize('path', ['hub', 'drain_events'])
def test_popped_reply_without_credit_is_put_back(consuming, path):
    # Held until credit returns, a recorded message could also be put back
    # by a restore past the visibility timeout, and be delivered twice.
    connection, queue, consumer, received = consuming
    channel = consumer.channel
    hub = Hub() if path == 'hub' else None
    try:
        pending, sock = pending_reply(consuming, hub, message='popped')
        if hub:
            tick(hub)
            run_until(hub, lambda: channel._in_poll is None)
        else:
            with pytest.raises(socket.timeout):
                connection.drain_events(timeout=0.01)
        assert sock.fileno() not in connection.transport.cycle._paused_brpop
        client = channel._create_client()
        assert client.hlen(channel.unacked_key) == 1
        assert client.llen(queue.name) == 1
        assert [item.payload for item in received] == [2]
        received[0].ack()
        if hub:
            run_until(hub, lambda: len(received) == 2)
        else:
            connection.drain_events(timeout=5)
        assert [item.payload for item in received] == [2, 3]
        received[1].ack()
        assert client.hlen(channel.unacked_key) == 0
    finally:
        if hub:
            hub.close()


@pytest.mark.parametrize('action', ['cancel', 'close'])
def test_pending_message_survives_cancel_or_close(consuming, action):
    connection, queue, consumer, received = consuming
    channel = consumer.channel
    hub = Hub()
    try:
        pending, sock = pending_reply(consuming, hub)
        tick(hub)
        assert_paused(connection, consumer, sock, hub)
        consumer.cancel()
        assert not channel.active_queues
        received[0].ack()
        if action == 'close':
            # close() must still drain and restore the pending reply.
            channel.close()
        else:
            # Credit must resume a pending reply even with no active queue.
            run_until(hub, lambda: channel._in_poll is None)
        assert [item.payload for item in received] == [1]
        with connection.channel() as recovery:
            message = queue(recovery).get(no_ack=True)
            assert message is not None
            assert message.payload == 2
            assert queue(recovery).get(no_ack=True) is None
    finally:
        hub.close()


def test_paused_channel_does_not_block_other_channels(consuming):
    connection, queue, consumer, received = consuming
    hub = Hub()
    other_queue = Queue(f'brpop-qos-other-{uuid4().hex}', durable=False)
    other_received = []
    try:
        pending, sock = pending_reply(consuming, hub)
        tick(hub)
        assert_paused(connection, consumer, sock, hub)
        with connection.channel() as other_channel:
            with Consumer(
                other_channel, queues=[other_queue], no_ack=True,
                callbacks=[lambda body, message: other_received.append(body)],
            ):
                Producer(connection).publish(
                    3, routing_key=other_queue.name, serializer='json')
                run_until(hub, lambda: bool(other_received))
                assert other_received == [3]
                assert [item.payload for item in received] == [1]
                assert consumer.channel._in_poll is pending
                assert_paused(connection, consumer, sock, hub)
            other_queue(other_channel).delete()
        received[0].ack()
        run_until(hub, lambda: len(received) == 2)
        assert received[1].payload == 2
        received[1].ack()
    finally:
        hub.close()
