import dataclasses
from enum import Enum


@dataclasses.dataclass(frozen=True)
class DialectConfig:
    """Describe a SIP2 dialect_config."""

    send_end_session: bool
    tz_spaces: bool


class Dialect(Enum):
    GENERIC_ILS = "GenericILS"
    AG_VERSO = "AutoGraphicsVerso"
    FOLIO = "TZSpaces"

    @property
    def config(self) -> DialectConfig:
        """Return the configuration for this dialect."""
        if self == Dialect.GENERIC_ILS:
            return DialectConfig(send_end_session=True, tz_spaces=False)
        elif self == Dialect.AG_VERSO:
            return DialectConfig(send_end_session=False, tz_spaces=False)
        elif self == Dialect.FOLIO:
            return DialectConfig(send_end_session=True, tz_spaces=True)
        else:
            raise NotImplementedError(f"Unknown dialect: {self}")
