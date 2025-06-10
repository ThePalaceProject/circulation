from __future__ import annotations

import sys

# TODO: Remove this when we drop support for Python 3.10
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class Axis360Formats(StrEnum):
    axis_now = "AxisNow"
    blio = "Blio"
    epub = "ePub"
    pdf = "PDF"
    acoustik = "Acoustik"
