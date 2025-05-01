from __future__ import annotations

import datetime

from palace.manager.util.datetime_helpers import utc_now


class MeasurementData:
    def __init__(
        self,
        quantity_measured: str,
        value: float | int | str,
        weight: float = 1,
        taken_at: datetime.datetime | None = None,
    ):
        if not quantity_measured:
            raise ValueError("quantity_measured is required.")
        if value is None:
            raise ValueError("measurement value is required.")
        self.quantity_measured = quantity_measured
        if not isinstance(value, float) and not isinstance(value, int):
            value = float(value)
        self.value = value
        self.weight = weight
        self.taken_at = taken_at or utc_now()

    def __repr__(self) -> str:
        return '<MeasurementData quantity="%s" value=%f weight=%d taken=%s>' % (
            self.quantity_measured,
            self.value,
            self.weight,
            self.taken_at,
        )
