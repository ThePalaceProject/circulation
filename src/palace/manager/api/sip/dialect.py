from __future__ import annotations

import dataclasses
from enum import Enum


@dataclasses.dataclass(frozen=True)
class DialectConfig:
    """Describe a SIP2 dialect_config."""

    send_end_session: bool
    tz_spaces: bool
    send_sc_status: bool = False


class Dialect(Enum):
    GENERIC_ILS = "GenericILS"
    AG_VERSO = "AutoGraphicsVerso"
    FOLIO = "TZSpaces"
    SIP_V2 = "SipV2Compliant"

    @property
    def config(self) -> DialectConfig:
        """Return the configuration for this dialect."""
        if self == Dialect.GENERIC_ILS:
            return DialectConfig(send_end_session=True, tz_spaces=False)
        elif self == Dialect.SIP_V2:
            return DialectConfig(
                send_end_session=True, tz_spaces=True, send_sc_status=True
            )
        elif self == Dialect.AG_VERSO:
            return DialectConfig(send_end_session=False, tz_spaces=False)
        elif self == Dialect.FOLIO:
            return DialectConfig(send_end_session=True, tz_spaces=True)
        else:
            raise NotImplementedError(f"Unknown dialect: {self}")

    @classmethod
    def form_options(cls) -> dict[Dialect, str]:
        return {
            cls.SIP_V2: "SIP v2.00 Compliant",
            cls.GENERIC_ILS: "Generic ILS",
            cls.AG_VERSO: "Auto-Graphics VERSO",
            cls.FOLIO: "Folio",
        }

    @classmethod
    def preferred(cls) -> str:
        """Return the preferred dialect."""
        return cls.form_options()[cls.SIP_V2]
