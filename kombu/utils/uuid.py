"""UUID utilities."""
from __future__ import absolute_import, unicode_literals

try:
    from fastuuid import uuid4
except ImportError:
    from uuid import uuid4


def uuid(_uuid=uuid4):
    """Generate unique id in UUID4 format.

    See Also:
        For now this is provided by :func:`uuid.uuid4`.
    """
    return str(_uuid())
