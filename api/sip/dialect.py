import dataclasses
from enum import Enum


@dataclasses.dataclass(frozen=True)
class DialectConfig:
    """Describe a SIP2 dialect."""

    sendEndSession: bool


class Dialect(Enum):
    GENERIC_ILS = DialectConfig(sendEndSession=True)
    AG_VERSO = DialectConfig(sendEndSession=False)
