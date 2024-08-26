"""Text Utilities."""

# flake8: noqa


from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Iterable, Iterator

from kombu import version_info_t


def escape_regex(p, white=''):
    # type: (str, str) -> str
    """Escape string for use within a regular expression."""
    # what's up with re.escape? that code must be neglected or something
    return ''.join(
        c if c.isalnum() or c in white else ('\\000' if c == '\000' else '\\' + c)
        for c in p
    )


def fmatch_iter(
    needle: str, haystack: Iterable[str], min_ratio: float = 0.6
) -> Iterator[tuple[float, str]]:
    """Fuzzy match: iteratively.

    Yields
    ------
        Tuple: of ratio and key.
    """
    for key in haystack:
        ratio = SequenceMatcher(None, needle, key).ratio()
        if ratio >= min_ratio:
            yield ratio, key


def fmatch_best(
    needle: str, haystack: Iterable[str], min_ratio: float = 0.6
) -> str | None:
    """Fuzzy match - Find best match (scalar)."""
    try:
        return sorted(
            fmatch_iter(needle, haystack, min_ratio),
            reverse=True,
        )[
            0
        ][1]
    except IndexError:
        return None


def version_string_as_tuple(version: str) -> version_info_t:
    """Parse a version string into its components and return a version_info_t tuple.

    The version string is expected to follow the pattern:
    'major.minor.micro[releaselevel][serial]'. Each component of the version
    is extracted and returned as a tuple in the format (major, minor, micro,
    releaselevel, serial).

    Args
    ----
        version (str): The version string to parse.

    Returns
    -------
        version_info_t: A tuple containing the parsed version components.

    Raises
    ------
        ValueError: If the version string is invalid and does not match the expected pattern.
    """
    pattern = r'^(\d+)'  # catching the major version (mandatory)
    pattern += r'(?:\.(\d+))?'  # optionally catching the minor version
    pattern += r'(?:\.(\d+))?'  # optionally catching the micro version
    pattern += r'(?:\.*([a-zA-Z+-][\da-zA-Z+-]*))?'  # optionally catching the release level (starting with a letter, + or -) after a dot
    pattern += r'(?:\.(.*))?'  # optionally catching the serial number after a dot

    # applying the regex pattern to the input version string
    match = re.match(pattern, version)

    if not match:
        raise ValueError(f"Invalid version string: {version}")

    # extracting the matched groups
    major = int(match.group(1))
    minor = int(match.group(2)) if match.group(2) else 0
    micro = int(match.group(3)) if match.group(3) else 0
    releaselevel = match.group(4) if match.group(4) else ''
    serial = match.group(5) if match.group(5) else ''

    return _unpack_version(major, minor, micro, releaselevel, serial)


def _unpack_version(
    major: str | int = 0,
    minor: str | int = 0,
    micro: str | int = 0,
    releaselevel: str = '',
    serial: str = '',
) -> version_info_t:
    return version_info_t(int(major), int(minor), int(micro), releaselevel, serial)


def _splitmicro(
    micro: str, releaselevel: str = '', serial: str = ''
) -> tuple[int, str, str]:
    for index, char in enumerate(micro):
        if not char.isdigit():
            break
    else:
        return int(micro or 0), releaselevel, serial
    return int(micro[:index]), micro[index:], serial
