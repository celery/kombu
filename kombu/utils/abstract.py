import abc

from typing import Any
from typing import Set  # noqa


def _hasattr(C: Any, attr: str) -> bool:
    return any(attr in B.__dict__ for B in C.__mro__)


class _AbstractClass(object, metaclass=abc.ABCMeta):
    __required_attributes__ = frozenset()  # type: frozenset

    @classmethod
    def _subclasshook_using(cls, parent: Any, C: Any):
        return (
            cls is parent and
            all(_hasattr(C, attr) for attr in cls.__required_attributes__)
        ) or NotImplemented

    @classmethod
    def register(cls, other: Any) -> Any:
        # we override `register` to return other for use as a decorator.
        type(cls).register(cls, other)
        return other


class Connection(_AbstractClass):
    ...


class Entity(_AbstractClass):
    ...


class Consumer(_AbstractClass):
    ...


class Producer(_AbstractClass):
    ...


class Messsage(_AbstractClass):
    ...


class Resource(_AbstractClass):
    ...
