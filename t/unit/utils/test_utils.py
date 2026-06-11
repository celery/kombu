from __future__ import annotations

import pytest

from kombu import version_info_t
from kombu.utils.text import version_string_as_tuple


def test_dir():
    import kombu

    assert dir(kombu)


@pytest.mark.parametrize(
    'version,expected',
    [
        ('3', version_info_t(3, 0, 0, '', '')),
        ('3.3', version_info_t(3, 3, 0, '', '')),
        ('3.3.1', version_info_t(3, 3, 1, '', '')),
        ('3.3.1a3', version_info_t(3, 3, 1, 'a3', '')),
        ('3.3.1.a3.40c32', version_info_t(3, 3, 1, 'a3', '40c32')),
        ('4.0.0+beta.3.47.g4f1a05b', version_info_t(
            4, 0, 0, '+beta', '3.47.g4f1a05b')),
        ('4.0.0-beta3.47.g4f1a05b', version_info_t(
            4, 0, 0, '-beta3', '47.g4f1a05b')),
        ('4.0.1-alpha.3+40c32', version_info_t(4, 0, 1, '-alpha', '3+40c32')),
        ('0+beta3.14159265', version_info_t(0, 0, 0, '+beta3', '14159265')),
    ],
)
def test_version_string_as_tuple(version, expected):
    assert version_string_as_tuple(version) == expected
