from copy import copy

from kombu.exceptions import NotBoundError


class Object(object):
    attrs = ()

    def __init__(self, *args, **kwargs):
        any = lambda v: v
        for name, type_ in self.attrs:
            value = kwargs.get(name)
            if value is not None:
                setattr(self, name, (type_ or any)(value))
            else:
                try:
                    getattr(self, name)
                except AttributeError:
                    setattr(self, name, None)

    def __copy__(self):
        return self.__class__(**dict((name, getattr(self, name))
                                        for name, _ in self.attrs))


class MaybeChannelBound(Object):
    """Mixin for classes that can be bound to an AMQP channel."""
    _channel = None
    _is_bound = False

    def __call__(self, channel):
        return self.bind(channel)

    def bind(self, channel):
        """Create copy of the instance that is bound to a channel."""
        return copy(self).maybe_bind(channel)

    def maybe_bind(self, channel):
        """Bind instance to channel if not already bound."""
        if not self.is_bound and channel:
            self._channel = channel
            self.when_bound()
            self._is_bound = True
        return self

    def revive(self, channel):
        """Revive channel afer connection re-established.

        Used by :meth:`~kombu.connection.BrokerConnection.ensure`.

        """
        if self.is_bound:
            self._channel = channel
            self.when_bound()

    def when_bound(self):
        """Callback called when the class is bound."""
        pass

    def __repr__(self, item=""):
        if self.is_bound:
            return "<bound %s of %s>" % (item or self.__class__.__name__,
                                         self.channel)
        return "<unbound %s>" % (item, )

    @property
    def is_bound(self):
        """Returns ``True`` if the entity is bound."""
        return self._is_bound and self._channel is not None

    @property
    def channel(self):
        if self._channel is None:
            raise NotBoundError(
                "Can't call method on %s not bound to a channel" % (
                    self.__class__.__name__))
        return self._channel
