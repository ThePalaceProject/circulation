from __future__ import annotations

from typing import Literal

from core.feed_protocol.serializer.opds import OPDS1Serializer
from core.feed_protocol.serializer.opds2 import OPDS2Serializer


def serializer_for(format: Literal["OPDS1"] | Literal["OPDS2"]):
    return OPDS1Serializer if format == "OPDS1" else OPDS2Serializer
