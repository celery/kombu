# -*- coding: utf-8 -*-
from difflib import SequenceMatcher
from typing import Iterator, Sequence, NamedTuple, Tuple

from kombu import version_info_t

fmatch_t = NamedTuple('fmatch_t', [('ratio', float), ('key', str)])


def escape_regex(p: str, white: str = ''):
    # what's up with re.escape? that code must be neglected or someting
    return ''.join(c if c.isalnum() or c in white
                   else ('\\000' if c == '\000' else '\\' + c)
                   for c in p)


def fmatch_iter(needle: str, haystack: Sequence[str],
                min_ratio: float = 0.6) -> Iterator[fmatch_t]:
    for key in haystack:
        ratio = SequenceMatcher(None, needle, key).ratio()
        if ratio >= min_ratio:
            yield fmatch_t(ratio, key)


def fmatch_best(needle: str, haystack: Sequence[str],
                min_ratio: float = 0.6) -> str:
    try:
        return sorted(
            fmatch_iter(needle, haystack, min_ratio), reverse=True,
        )[0][1]
    except IndexError:
        pass


def version_string_as_tuple(s: str) -> version_info_t:
    v = _unpack_version(*s.split('.'))
    # X.Y.3a1 -> (X, Y, 3, 'a1')
    if isinstance(v.micro, str):
        v = version_info_t(v.major, v.minor, *_splitmicro(*v[2:]))
    # X.Y.3a1-40 -> (X, Y, 3, 'a1', '40')
    if not v.serial and v.releaselevel and '-' in v.releaselevel:
        v = version_info_t(*list(v[0:3]) + v.releaselevel.split('-'))
    return v


def _unpack_version(major: int, minor: int = 0, micro: int = 0,
                    releaselevel: str = '',
                    serial: str = '') -> version_info_t:
    return version_info_t(int(major), int(minor), micro, releaselevel, serial)


def _splitmicro(micro: str,
                releaselevel: str = '',
                serial: str = '') -> Tuple[int, str, str]:
    for index, char in enumerate(micro):
        if not char.isdigit():
            break
    else:
        return int(micro or 0), releaselevel, serial
    return int(micro[:index]), micro[index:], serial
