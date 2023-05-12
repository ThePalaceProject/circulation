import dataclasses
from enum import Enum


@dataclasses.dataclass(frozen=True)
class DialectConfig:
    """Describe a SIP2 dialect_config."""

    sendEndSession: bool


class Dialect(Enum):
    GENERIC_ILS = "GenericILS"
    AG_VERSO = "AutoGraphicsVerso"

    @property
    def config(self) -> DialectConfig:
        """Return the configuration for this dialect."""
        if self == Dialect.GENERIC_ILS:
            return DialectConfig(sendEndSession=True)
        elif self == Dialect.AG_VERSO:
            return DialectConfig(sendEndSession=False)
        else:
            raise NotImplementedError(f"Unknown dialect: {self}")
