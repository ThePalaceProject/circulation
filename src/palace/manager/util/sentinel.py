from __future__ import annotations

from enum import Enum


class SentinelType(Enum):
    """
    Sentinel values used throughout the Palace codebase.

    If https://peps.python.org/pep-0661/ is accepted, we should update this is use
    a proper sentinel value. For now, we use this enum, since we can type check it.

    This solution is based on discussion here:
    https://github.com/python/typing/issues/236#issuecomment-227180301

    It can be type hinted as: Literal[SentinelType.NotGiven]
    """

    NotGiven = "NotGiven"
    """
    We use this so we can differentiate between a variable that is not given
    and a variable that is given as None.
    """

    ClearValue = "ClearValue"
    """
    We use this to indicate that a variable should be cleared (set to None),
    when passing None would use the default value.
    """
