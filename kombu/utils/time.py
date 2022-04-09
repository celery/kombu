"""Time Utilities."""
from typing import Optional, Union

__all__ = ('maybe_s_to_ms',)


def maybe_s_to_ms(v: Optional[Union[int, float]]) -> Optional[int]:
    """Convert seconds to milliseconds, but return None for None."""
    return int(float(v) * 1000.0) if v is not None else v
