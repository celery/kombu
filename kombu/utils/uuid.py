"""UUID utilities."""
from uuid import uuid4
from typing import Callable


def uuid(*, _uuid: Callable = uuid4) -> str:
    """Generate unique id in UUID4 format.

    See Also:
        For now this is provided by :func:`uuid.uuid4`.
    """
    return str(_uuid())
