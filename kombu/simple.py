"""Simple messaging interface."""
import socket
from collections import deque
from time import monotonic
from queue import Empty
from typing import Any, Mapping
from . import entity
from . import messaging
from .connection import maybe_channel
from .types import ChannelArgT, ConsumerT, ProducerT, MessageT, SimpleQueueT

__all__ = ['SimpleQueue', 'SimpleBuffer']


class SimpleBase(SimpleQueueT):
    Empty = Empty
    _consuming: bool = False

    def __enter__(self) -> SimpleQueueT:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def __init__(self,
                 channel: ChannelArgT,
                 producer: ProducerT,
                 consumer: ConsumerT,
                 no_ack: bool = False) -> None:
        self.channel = maybe_channel(channel)
        self.producer = producer
        self.consumer = consumer
        self.no_ack = no_ack
        self.queue = self.consumer.queues[0]
        self.buffer = deque()
        self.consumer.register_callback(self._receive)

    def get(self, block: bool = True, timeout: float = None) -> MessageT:
        if not block:
            return self.get_nowait()
        self._consume()
        elapsed = 0.0
        remaining = timeout
        while True:
            time_start = monotonic()
            if self.buffer:
                return self.buffer.popleft()
            try:
                self.channel.connection.client.drain_events(
                    timeout=timeout and remaining)
            except socket.timeout:
                raise self.Empty()
            elapsed += monotonic() - time_start
            remaining = timeout and timeout - elapsed or None

    def get_nowait(self) -> MessageT:
        m = self.queue.get(no_ack=self.no_ack)
        if not m:
            raise self.Empty()
        return m

    def put(self, message: Any,
            serializer: str = None,
            headers: Mapping = None,
            compression: str = None,
            routing_key: str = None,
            **kwargs) -> None:
        self.producer.publish(message,
                              serializer=serializer,
                              routing_key=routing_key,
                              headers=headers,
                              compression=compression,
                              **kwargs)

    def clear(self) -> int:
        return self.consumer.purge()

    def qsize(self) -> int:
        _, size, _ = self.queue.queue_declare(passive=True)
        return size

    def close(self) -> None:
        self.consumer.cancel()

    def _receive(self, message_data: Any, message: MessageT) -> None:
        self.buffer.append(message)

    def _consume(self) -> None:
        if not self._consuming:
            self.consumer.consume(no_ack=self.no_ack)
            self._consuming = True

    def __len__(self) -> int:
        """`len(self) -> self.qsize()`."""
        return self.qsize()

    def __bool__(self) -> bool:
        return True


class SimpleQueue(SimpleBase):
    """Simple API for persistent queues."""

    no_ack: bool = False
    queue_opts: Mapping = {}
    exchange_opts: Mapping = {'type': 'direct'}

    def __init__(
            self,
            channel: ChannelArgT,
            name: str,
            no_ack: bool = None,
            queue_opts: Mapping = None,
            exchange_opts: Mapping = None,
            serializer: str = None,
            compression: str = None,
            **kwargs) -> None:
        queue = name
        all_queue_opts = dict(self.queue_opts)
        if queue_opts:
            all_queue_opts.update(queue_opts)
        all_exchange_opts = dict(self.exchange_opts)
        if exchange_opts:
            all_exchange_opts.update(exchange_opts)
        if no_ack is None:
            no_ack = self.no_ack
        if not isinstance(queue, entity.Queue):
            exchange = entity.Exchange(name, **all_exchange_opts)
            queue = entity.Queue(name, exchange, name, **all_queue_opts)
            routing_key = name
        else:
            name = queue.name
            exchange = queue.exchange
            routing_key = queue.routing_key
        consumer = messaging.Consumer(channel, [queue])
        producer = messaging.Producer(channel, exchange,
                                      serializer=serializer,
                                      routing_key=routing_key,
                                      compression=compression)
        super().__init__(channel, producer, consumer, no_ack)


class SimpleBuffer(SimpleQueue):
    """Simple API for ephemeral queues."""

    no_ack = True
    queue_opts = dict(durable=False,
                      auto_delete=True)
    exchange_opts = dict(durable=False,
                         delivery_mode='transient',
                         auto_delete=True)
