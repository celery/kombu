from __future__ import absolute_import

__all__ = ['maybe_s_to_ms']


def maybe_s_to_ms(v):
    return int(float(v) * 1000.0) if v is not None else v
