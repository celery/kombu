from abc import abstractmethod
from typing import (
    _Protocol, AnyStr, SupportsFloat, SupportsInt, Union,
)

Float = Union[SupportsInt, SupportsFloat, AnyStr]
Int = Union[SupportsInt, AnyStr]

Port = Union[SupportsInt, str]

AnyBuffer = Union[AnyStr, memoryview]


class SupportsFileno(_Protocol):
    __slots__ = ()

    @abstractmethod
    def __fileno__(self) -> int:
        ...

Fd = Union[int, SupportsFileno]
