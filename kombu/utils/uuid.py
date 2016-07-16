from __future__ import absolute_import, unicode_literals

from uuid import uuid4


def uuid(_uuid=uuid4):
    """Generate a unique id, having - hopefully - a very small chance of
    collision.

    See Also:
        For now this is provided by :func:`uuid.uuid4`.
    """
    return str(_uuid())
