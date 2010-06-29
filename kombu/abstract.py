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


def assert_is_bound(fun):

    def if_bound(self, *args, **kwargs):
        if self.is_bound:
            return fun(self, *args, **kwargs)
        raise NotBoundError(
                "Can't call %s on %s not bound to a channel" % (
                    fun.__name__,
                    self.__class__.__name__))
    if_bound.__name__ = fun.__name__
    if_bound.__doc__ = fun.__doc__
    if_bound.__module__ = fun.__module__
    if_bound.__dict__.update(fun.__dict__)
    if_bound.func_name = fun.__name__

    return if_bound


class MaybeChannelBound(Object):
    """Mixin for classes that can be bound to an AMQP channel."""
    channel = None
    _is_bound = False

    def __call__(self, channel):
        return self.bind(channel)

    def bind(self, channel):
        """Create copy of the instance that is bound to a channel."""
        return copy(self).maybe_bind(channel)

    def maybe_bind(self, channel):
        """Bind instance to channel if not already bound."""
        if not self.is_bound and channel:
            self.channel = channel
            self.when_bound()
            self._is_bound = True
        return self

    def when_bound(self):
        """Callback called when the class is bound."""
        pass

    @property
    def is_bound(self):
        """Returns ``True`` if the entity is bound."""
        return self._is_bound and self.channel is not None

    def __repr__(self, item=""):
        if self.is_bound:
            return "<bound %s of %s>" % (item or self.__class__.__name__,
                                         self.channel)
        return "<unbound %s>" % (item, )
