from __future__ import annotations

from pydantic import AwareDatetime, Field

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.util.datetime_helpers import utc_now


class MeasurementData(BaseFrozenData):
    quantity_measured: str
    value: float
    weight: float = 1.0
    taken_at: AwareDatetime = Field(default_factory=utc_now)
