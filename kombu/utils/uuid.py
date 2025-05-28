from __future__ import annotations

from typing import Literal
from uuid import uuid4

try:
    # Python 3.11 or later
    from uuid import uuid7
except ImportError:
    # Fallback for older Python versions
    def uuid7():
        """Fallback to UUID4 if UUID7 is not available."""
        return uuid4()

def generate_uuid(version: Literal[4, 7] = 7) -> str:
    """Generate a unique ID based on the specified UUID version (4 or 7).
    
    Parameters
    ----------
    version : Literal[4, 7]
        The UUID version to use for generating the unique ID. Defaults to 7.
        If UUID7 is unavailable, it falls back to UUID4 regardless of the input.
    
    Returns
    -------
    str
        A string representation of the generated UUID.
    """
    if version == 7 and 'uuid7' in globals():
        return str(uuid7())
    elif version == 4:
        return str(uuid4())
    else:
        raise ValueError("Invalid UUID version. Please choose either 4 or 7.")
