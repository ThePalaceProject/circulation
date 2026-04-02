from __future__ import annotations

from pydantic import AwareDatetime

from palace.util.datetime_helpers import utc_now

from palace.manager.data_layer.base.frozen import BaseFrozenData


class MeasurementData(BaseFrozenData):
    quantity_measured: str
    value: float
    weight: float = 1.0
    taken_at: AwareDatetime | None = None
