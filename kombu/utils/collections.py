"""Custom maps, sequences, etc."""
from typing import Any, Union


class HashedSeq(list):
    """Hashed Sequence.

    Type used for hash() to make sure the hash is not generated
    multiple times.
    """

    __slots__ = 'hashvalue'

    def __init__(self, *seq) -> None:
        self[:] = seq
        self.hashvalue = hash(seq)

    def __hash__(self) -> int:
        return self.hashvalue


def eqhash(o: Any) -> Union[int, HashedSeq]:
    """Call ``obj.__eqhash__``."""
    try:
        return o.__eqhash__()
    except AttributeError:
        return hash(o)


class EqualityDict(dict):
    """Dict using the eq operator for keying."""

    def __getitem__(self, key: Any) -> Any:
        h = eqhash(key)
        if h not in self:
            return self.__missing__(key)
        return dict.__getitem__(self, h)

    def __setitem__(self, key: Any, value: Any) -> None:
        dict.__setitem__(self, eqhash(key), value)

    def __delitem__(self, key: Any) -> None:
        dict.__delitem__(self, eqhash(key))

    def __missing__(self, key: Any) -> Any:
        raise KeyError(key)
