"""
kombu.abstract
==============

Object utilities.

"""

import amqp.abstract

from copy import copy
from typing import Any, Dict
from typing import Sequence, Tuple  # noqa

from .connection import maybe_channel
from .exceptions import NotBoundError
from .utils import ChannelPromise

__all__ = ['Object', 'MaybeChannelBound']


def unpickle_dict(cls: Any, kwargs: Dict) -> Any:
    return cls(**kwargs)


def _any(v: Any) -> Any:
    return v


class Object:
    """Common base class supporting automatic kwargs->attributes handling,
    and cloning."""
    attrs = ()  # type: Sequence[Tuple[str, Any]]

    def __init__(self, *args, **kwargs) -> None:
        for name, type_ in self.attrs:
            value = kwargs.get(name)
            if value is not None:
                setattr(self, name, (type_ or _any)(value))
            else:
                try:
                    getattr(self, name)
                except AttributeError:
                    setattr(self, name, None)

    def as_dict(self, recurse: bool=False) -> Dict:
        def f(obj: Any, type: Any) -> Any:
            if recurse and isinstance(obj, Object):
                return obj.as_dict(recurse=True)
            return type(obj) if type else obj
        return {
            attr: f(getattr(self, attr), type) for attr, type in self.attrs
        }

    def __reduce__(self) -> Any:
        return unpickle_dict, (self.__class__, self.as_dict())

    def __copy__(self) -> Any:
        return self.__class__(**self.as_dict())


class MaybeChannelBound(Object):
    """Mixin for classes that can be bound to an AMQP channel."""
    _channel = None    # type: amqp.abstract.Channel
    _is_bound = False  # type: bool

    #: Defines whether maybe_declare can skip declaring this entity twice.
    can_cache_declaration = False

    def __call__(self, channel: amqp.abstract.Channel) -> 'MaybeChannelBound':
        """`self(channel) -> self.bind(channel)`"""
        return self.bind(channel)

    def bind(self, channel: amqp.abstract.Channel) -> 'MaybeChannelBound':
        """Create copy of the instance that is bound to a channel."""
        return copy(self).maybe_bind(channel)

    def maybe_bind(self,
                   channel: amqp.abstract.Channel) -> 'MaybeChannelBound':
        """Bind instance to channel if not already bound."""
        if not self.is_bound and channel:
            self._channel = maybe_channel(channel)
            self.when_bound()
            self._is_bound = True
        return self

    def revive(self, channel: amqp.abstract.Channel) -> None:
        """Revive channel after the connection has been re-established.

        Used by :meth:`~kombu.Connection.ensure`.

        """
        if self.is_bound:
            self._channel = channel
            self.when_bound()

    def when_bound(self) -> None:
        """Callback called when the class is bound."""
        ...

    def __repr__(self) -> str:
        return self._repr_entity(type(self).__name__)

    def _repr_entity(self, item: str='') -> str:
        item = item or type(self).__name__
        if self.is_bound:
            return '<{0} bound to chan:{1}>'.format(
                item or type(self).__name__, self.channel.channel_id)
        return '<unbound {0}>'.format(item)

    @property
    def is_bound(self) -> bool:
        """Flag set if the channel is bound."""
        return self._is_bound and self._channel is not None

    @property
    def channel(self) -> amqp.abstract.Channel:
        """Current channel if the object is bound."""
        channel = self._channel
        if channel is None:
            raise NotBoundError(
                "Can't call method on {0} not bound to a channel".format(
                    type(self).__name__))
        if isinstance(channel, ChannelPromise):
            channel = self._channel = channel()
        return channel
