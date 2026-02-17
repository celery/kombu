import abc
import logging
from collections import Hashable, deque
from numbers import Number
from typing import (
    Any, Callable, Dict, Iterable, List, Mapping, Optional,
    Sequence, TypeVar, Tuple, Union,
)
from typing import Set  # noqa
from amqp.types import ChannelT, MessageT as _MessageT, TransportT
from amqp.spec import queue_declare_ok_t


def _hasattr(C: Any, attr: str) -> bool:
    return any(attr in B.__dict__ for B in C.__mro__)


class _AbstractClass(abc.ABCMeta):
    __required_attributes__ = frozenset()  # type: frozenset

    @classmethod
    def _subclasshook_using(cls, parent: Any, C: Any):
        return (
            cls is parent and
            all(_hasattr(C, attr) for attr in cls.__required_attributes__)
        ) or NotImplemented


class Revivable(metaclass=_AbstractClass):

    async def revive(self, channel: ChannelT) -> None:
        ...


class ClientT(Revivable, metaclass=_AbstractClass):

    hostname: str
    userid: str
    password: str
    ssl: Any
    login_method: str
    port: int = None
    virtual_host: str = '/'
    connect_timeout: float = 5.0
    alt: Sequence[str]

    uri_prefix: str = None
    declared_entities: Set['EntityT'] = None
    cycle: Iterable
    transport_options: Mapping
    failover_strategy: str
    heartbeat: float = None
    resolve_aliases: Mapping[str, str] = resolve_aliases
    failover_strategies: Mapping[str, Callable] = failover_strategies

    @property
    @abc.abstractmethod
    def default_channel(self) -> ChannelT:
        ...

    def __init__(
            self,
            hostname: str = 'localhost',
            userid: str = None,
            password: str = None,
            virtual_host: str = None,
            port: int = None,
            insist: bool = False,
            ssl: Any = None,
            transport: Union[type, str] = None,
            connect_timeout: float = 5.0,
            transport_options: Mapping = None,
            login_method: str = None,
            uri_prefix: str = None,
            heartbeat: float = None,
            failover_strategy: str = 'round-robin',
            alternates: Sequence[str] = None,
            **kwargs) -> None:
        ...

    @abc.abstractmethod
    def switch(self, url: str) -> None:
        ...

    @abc.abstractmethod
    def maybe_switch_next(self) -> None:
        ...

    @abc.abstractmethod
    async def connect(self) -> None:
        ...

    @abc.abstractmethod
    def channel(self) -> ChannelT:
        ...

    @abc.abstractmethod
    async def heartbeat_check(self, rate: int = 2) -> None:
        ...

    @abc.abstractmethod
    async def drain_events(self, timeout: float = None, **kwargs) -> None:
        ...

    @abc.abstractmethod
    async def maybe_close_channel(self, channel: ChannelT) -> None:
        ...

    @abc.abstractmethod
    async def collect(self, socket_timeout: float = None) -> None:
        ...

    @abc.abstractmethod
    async def release(self) -> None:
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        ...

    @abc.abstractmethod
    async def ensure_connection(
            self,
            errback: Callable = None,
            max_retries: int = None,
            interval_start: float = 2.0,
            interval_step: float = 2.0,
            interval_max: float = 30.0,
            callback: Callable = None,
            reraise_as_library_errors: bool = True) -> 'ClientT':
        ...

    @abc.abstractmethod
    def completes_cycle(self, retries: int) -> bool:
        ...

    @abc.abstractmethod
    async def ensure(
            self, obj: Revivable, fun: Callable,
            errback: Callable = None,
            max_retries: int = None,
            interval_start: float = 1.0,
            interval_step: float = 1.0,
            interval_max: float = 1.0,
            on_revive: Callable = None) -> Any:
        ...

    @abc.abstractmethod
    def autoretry(self, fun: Callable,
                  channel: ChannelT = None, **ensure_options) -> Any:
        ...

    @abc.abstractmethod
    def create_transport(self) -> TransportT:
        ...

    @abc.abstractmethod
    def get_transport_cls(self) -> type:
        ...

    @abc.abstractmethod
    def clone(self, **kwargs) -> 'ClientT':
        ...

    @abc.abstractmethod
    def get_heartbeat_interval(self) -> float:
        ...

    @abc.abstractmethod
    def info(self) -> Mapping[str, Any]:
        ...

    @abc.abstractmethod
    def as_uri(
            self,
            *,
            include_password: bool = False,
            mask: str = '**',
            getfields: Callable = None) -> str:
        ...

    @abc.abstractmethod
    def Pool(self, limit: int = None, **kwargs) -> 'ResourceT':
        ...

    @abc.abstractmethod
    def ChannelPool(self, limit: int = None, **kwargs) -> 'ResourceT':
        ...

    @abc.abstractmethod
    def Producer(self, channel: ChannelT = None,
                 *args, **kwargs) -> 'ProducerT':
        ...

    @abc.abstractmethod
    def Consumer(
            self,
            queues: Sequence['QueueT'] = None,
            channel: ChannelT = None,
            *args, **kwargs) -> 'ConsumerT':
        ...

    @abc.abstractmethod
    def SimpleQueue(
            self, name: str,
            no_ack: bool = None,
            queue_opts: Mapping = None,
            exchange_opts: Mapping = None,
            channel: ChannelT = None,
            **kwargs) -> 'SimpleQueueT':
        ...


class EntityT(Hashable, Revivable, metaclass=_AbstractClass):

    @property
    @abc.abstractmethod
    def can_cache_declaration(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def is_bound(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def channel(self) -> ChannelT:
        ...

    @abc.abstractmethod
    def __call__(self, channel: ChannelT) -> 'EntityT':
        ...

    @abc.abstractmethod
    def bind(self, channel: ChannelT) -> 'EntityT':
        ...

    @abc.abstractmethod
    def maybe_bind(self, channel: Optional[ChannelT]) -> 'EntityT':
        ...

    @abc.abstractmethod
    def when_bound(self) -> None:
        ...

    @abc.abstractmethod
    def as_dict(self, recurse: bool = False) -> Dict:
        ...


ChannelArgT = TypeVar('ChannelArgT', ChannelT, ClientT)


class ExchangeT(EntityT, metaclass=_AbstractClass):

    name: str
    type: str
    channel: ChannelT
    arguments: Mapping
    durable: bool = True
    auto_delete: bool = False
    delivery_mode: int
    no_declare: bool = False
    passive: bool = False

    def __init__(
            self,
            name: str = '',
            type: str = '',
            channel: ChannelArgT = None,
            **kwargs) -> None:
        ...

    @abc.abstractmethod
    async def declare(
            self,
            nowait: bool = False,
            passive: bool = None,
            channel: ChannelT = None) -> None:
        ...

    @abc.abstractmethod
    async def bind_to(
            self,
            exchange: Union[str, 'ExchangeT'] = '',
            routing_key: str = '',
            arguments: Mapping = None,
            nowait: bool = False,
            channel: ChannelT = None,
            **kwargs) -> None:
        ...

    @abc.abstractmethod
    async def unbind_from(
            self,
            source: Union[str, 'ExchangeT'] = '',
            routing_key: str = '',
            nowait: bool = False,
            arguments: Mapping = None,
            channel: ChannelT = None) -> None:
        ...

    @abc.abstractmethod
    async def publish(
            self, message: Union[MessageT, str, bytes],
            routing_key: str = None,
            mandatory: bool = False,
            immediate: bool = False,
            exchange: str = None) -> None:
        ...

    @abc.abstractmethod
    async def delete(
            self,
            if_unused: bool = False,
            nowait: bool = False) -> None:
        ...

    @abc.abstractmethod
    def binding(
            self,
            routing_key: str = '',
            arguments: Mapping = None,
            unbind_arguments: Mapping = None) -> BindingT:
        ...

    @abc.abstractmethod
    def Message(
            self, body: Any,
            delivery_mode: Union[str, int] = None,
            properties: Mapping = None,
            **kwargs) -> Any:
        ...


class QueueT(EntityT, metaclass=_AbstractClass):

    name: str = ''
    exchange: ExchangeT
    routing_key: str = ''
    alias: str

    bindings: Sequence['BindingT'] = None

    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    no_ack: bool = False
    no_declare: bool = False
    expires: float
    message_ttl: float
    max_length: int
    max_length_bytes: int
    max_priority: int

    queue_arguments: Mapping
    binding_arguments: Mapping
    consumer_arguments: Mapping

    def __init__(
            self,
            name: str = '',
            exchange: ExchangeT = None,
            routing_key: str = '',
            channel: ChannelT = None,
            bindings: Sequence['BindingT'] = None,
            on_declared: Callable = None,
            **kwargs) -> None:
        ...

    @abc.abstractmethod
    async def declare(self,
                      nowait: bool = False,
                      channel: ChannelT = None) -> str:
        ...

    @abc.abstractmethod
    async def queue_declare(
            self,
            nowait: bool = False,
            passive: bool = False,
            channel: ChannelT = None) -> queue_declare_ok_t:
        ...

    @abc.abstractmethod
    async def queue_bind(
            self,
            nowait: bool = False,
            channel: ChannelT = None) -> None:
        ...

    @abc.abstractmethod
    async def bind_to(
            self,
            exchange: Union[str, ExchangeT] = '',
            routing_key: str = '',
            arguments: Mapping = None,
            nowait: bool = False,
            channel: ChannelT = None) -> None:
        ...

    @abc.abstractmethod
    async def get(
            self,
            no_ack: bool = None,
            accept: Set[str] = None) -> MessageT:
        ...

    @abc.abstractmethod
    async def purge(self, nowait: bool = False) -> int:
        ...

    @abc.abstractmethod
    async def consume(
            self,
            consumer_tag: str = '',
            callback: Callable = None,
            no_ack: bool = None,
            nowait: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def cancel(self, consumer_tag: str) -> None:
        ...

    @abc.abstractmethod
    async def delete(
            self,
            if_unused: bool = False,
            if_empty: bool = False,
            nowait: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def queue_unbind(
            self,
            arguments: Mapping = None,
            nowait: bool = False,
            channel: ChannelT = None) -> None:
        ...

    @abc.abstractmethod
    async def unbind_from(
            self,
            exchange: str = '',
            routing_key: str = '',
            arguments: Mapping = None,
            nowait: bool = False,
            channel: ChannelT = None) -> None:
        ...

    @classmethod
    @abc.abstractmethod
    def from_dict(
            self, queue: str,
            exchange: str = None,
            exchange_type: str = None,
            binding_key: str = None,
            routing_key: str = None,
            delivery_mode: Union[int, str] = None,
            bindings: Sequence = None,
            durable: bool = None,
            queue_durable: bool = None,
            exchange_durable: bool = None,
            auto_delete: bool = None,
            queue_auto_delete: bool = None,
            exchange_auto_delete: bool = None,
            exchange_arguments: Mapping = None,
            queue_arguments: Mapping = None,
            binding_arguments: Mapping = None,
            consumer_arguments: Mapping = None,
            exclusive: bool = None,
            no_ack: bool = None,
            **options) -> 'QueueT':
        ...


class BindingT(metaclass=_AbstractClass):

    exchange: ExchangeT
    routing_key: str
    arguments: Mapping
    unbind_arguments: Mapping

    def __init__(
            self,
            exchange: ExchangeT = None,
            routing_key: str = '',
            arguments: Mapping = None,
            unbind_arguments: Mapping = None) -> None:
        ...

    def declare(self, channel: ChannelT, nowait: bool = False) -> None:
        ...

    def bind(
            self, entity: EntityT,
            nowait: bool = False,
            channel: ChannelT = None) -> None:
        ...

    def unbind(
            self, entity: EntityT,
            nowait: bool = False,
            channel: ChannelT = None) -> None:
        ...


class ConsumerT(Revivable, metaclass=_AbstractClass):

    ContentDisallowed: type

    channel: ChannelT
    queues: Sequence[QueueT]
    no_ack: bool
    auto_declare: bool = True
    callbacks: Sequence[Callable]
    on_message: Callable
    on_decode_error: Callable
    accept: Set[str]
    prefetch_count: int = None
    tag_prefix: str

    @property
    @abc.abstractmethod
    def connection(self) -> ClientT:
        ...

    def __init__(
            self,
            channel: ChannelT,
            queues: Sequence[QueueT] = None,
            no_ack: bool = None,
            auto_declare: bool = None,
            callbacks: Sequence[Callable] = None,
            on_decode_error: Callable = None,
            on_message: Callable = None,
            accept: Sequence[str] = None,
            prefetch_count: int = None,
            tag_prefix: str = None) -> None:
        ...

    @abc.abstractmethod
    async def declare(self) -> None:
        ...

    @abc.abstractmethod
    def register_callback(self, callback: Callable) -> None:
        ...

    @abc.abstractmethod
    def __enter__(self) -> 'ConsumerT':
        ...

    @abc.abstractmethod
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        ...

    @abc.abstractmethod
    async def __aenter__(self) -> 'ConsumerT':
        ...

    @abc.abstractmethod
    async def __aexit__(self, *exc_info) -> None:
        ...

    @abc.abstractmethod
    async def add_queue(self, queue: QueueT) -> QueueT:
        ...

    @abc.abstractmethod
    async def consume(self, no_ack: bool = None) -> None:
        ...

    @abc.abstractmethod
    async def cancel(self) -> None:
        ...

    @abc.abstractmethod
    async def cancel_by_queue(self, queue: Union[QueueT, str]) -> None:
        ...

    @abc.abstractmethod
    def consuming_from(self, queue: Union[QueueT, str]) -> bool:
        ...

    @abc.abstractmethod
    async def purge(self) -> int:
        ...

    @abc.abstractmethod
    async def flow(self, active: bool) -> None:
        ...

    @abc.abstractmethod
    async def qos(self,
                  prefetch_size: int = 0,
                  prefetch_count: int = 0,
                  apply_global: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def recover(self, requeue: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def receive(self, body: Any, message: MessageT) -> None:
        ...


class ProducerT(Revivable, metaclass=_AbstractClass):

    exchange: ExchangeT
    routing_key: str = ''
    serializer: str
    compression: str
    auto_declare: bool = True
    on_return: Callable = None

    __connection__: ClientT

    channel: ChannelT

    @property
    @abc.abstractmethod
    def connection(self) -> ClientT:
        ...

    def __init__(self,
                 channel: ChannelArgT,
                 exchange: ExchangeT = None,
                 routing_key: str = None,
                 serializer: str = None,
                 auto_declare: bool = None,
                 compression: str = None,
                 on_return: Callable = None) -> None:
        ...

    @abc.abstractmethod
    async def declare(self) -> None:
        ...

    @abc.abstractmethod
    async def maybe_declare(self, entity: EntityT,
                            retry: bool = False, **retry_policy) -> None:
        ...

    @abc.abstractmethod
    async def publish(
            self, body: Any,
            routing_key: str = None,
            delivery_mode: Union[int, str] = None,
            mandatory: bool = False,
            immediate: bool = False,
            priority: int = 0,
            content_type: str = None,
            content_encoding: str = None,
            serializer: str = None,
            headers: Mapping = None,
            compression: str = None,
            exchange: Union[ExchangeT, str] = None,
            retry: bool = False,
            retry_policy: Mapping = None,
            declare: Sequence[EntityT] = None,
            expiration: Number = None,
            **properties) -> None:
        ...

    @abc.abstractmethod
    def __enter__(self) -> 'ProducerT':
        ...

    @abc.abstractmethod
    async def __aenter__(self) -> 'ProducerT':
        ...

    @abc.abstractmethod
    def __exit__(self, *exc_info) -> None:
        ...

    @abc.abstractmethod
    async def __aexit__(self, *exc_info) -> None:
        ...

    @abc.abstractmethod
    def release(self) -> None:
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...


class MessageT(_MessageT):

    MessageStateError: type

    _state: str
    channel: ChannelT = None
    delivery_tag: str
    content_type: str
    content_encoding: str
    delivery_info: Mapping
    headers: Mapping
    properties: Mapping
    accept: Set[str]
    body: Any
    errors: List[Any]

    @property
    @abc.abstractmethod
    def acknowledged(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def payload(self) -> Any:
        ...

    def __init__(
            self,
            body: Any = None,
            delivery_tag: str = None,
            content_type: str = None,
            content_encoding: str = None,
            delivery_info: Mapping = None,
            properties: Mapping = None,
            headers: Mapping = None,
            postencode: str = None,
            accept: Set[str] = None,
            channel: ChannelT = None,
            **kwargs) -> None:
        ...

    @abc.abstractmethod
    async def ack(self, multiple: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def ack_log_error(
            self, logger: logging.Logger, errors: Tuple[type, ...],
            multiple: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def reject_log_error(
            self, logger: logging.Logger, errors: Tuple[type, ...],
            requeue: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def reject(self, requeue: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def requeue(self) -> None:
        ...

    @abc.abstractmethod
    def decode(self) -> Any:
        ...


class ResourceT(metaclass=_AbstractClass):

    LimitedExceeeded: type

    close_after_fork: bool = False

    @property
    @abc.abstractmethod
    def limit(self) -> int:
        ...

    def __init__(self,
                 limit: int = None,
                 preload: int = None,
                 close_after_fork: bool = None) -> None:
        ...

    @abc.abstractmethod
    def setup(self) -> None:
        ...

    @abc.abstractmethod
    def acquire(self, block: bool = False, timeout: int = None):
        ...

    @abc.abstractmethod
    def prepare(self, resource: Any) -> Any:
        ...

    @abc.abstractmethod
    def close_resource(self, resource: Any) -> None:
        ...

    @abc.abstractmethod
    def release_resource(self, resource: Any) -> None:
        ...

    @abc.abstractmethod
    def replace(self, resource: Any) -> None:
        ...

    @abc.abstractmethod
    def release(self, resource: Any) -> None:
        ...

    @abc.abstractmethod
    def collect_resource(self, resource: Any) -> None:
        ...

    @abc.abstractmethod
    def force_close_all(self) -> None:
        ...

    @abc.abstractmethod
    def resize(
            self, limit: int,
            force: bool = False,
            ignore_errors: bool = False,
            reset: bool = False) -> None:
        ...


class SimpleQueueT(metaclass=_AbstractClass):

    Empty: type

    channel: ChannelT
    producer: ProducerT
    consumer: ConsumerT
    no_ack: bool = False
    queue: QueueT
    buffer: deque

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
        ...

    @abc.abstractmethod
    def __enter__(self) -> 'SimpleQueueT':
        ...

    @abc.abstractmethod
    def __exit__(self, *exc_info) -> None:
        ...

    @abc.abstractmethod
    def get(self, block: bool = True, timeout: float = None) -> MessageT:
        ...

    @abc.abstractmethod
    def get_nowait(self) -> MessageT:
        ...

    @abc.abstractmethod
    def put(self, message: Any,
            serializer: str = None,
            headers: Mapping = None,
            compression: str = None,
            routing_key: str = None,
            **kwargs) -> None:
        ...

    @abc.abstractmethod
    def clear(self) -> int:
        ...

    @abc.abstractmethod
    def qsize(self) -> int:
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...

    @abc.abstractmethod
    def __len__(self) -> int:
        ...

    @abc.abstractmethod
    def __bool__(self) -> bool:
        ...
