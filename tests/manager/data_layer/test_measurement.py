import datetime

from freezegun import freeze_time

from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.util.datetime_helpers import utc_now


class TestMeasurementData:
    def test_taken_at(self) -> None:
        """Test that the taken_at is set correctly."""

        # If taken_at is given, it's converted to a datetime.datetime with the correct date
        measurement = MeasurementData.model_validate(
            {
                "quantity_measured": "quality",
                "value": 25.0,
                "taken_at": "2023-10-01T00:00:00+00:00",
            }
        )
        assert measurement.taken_at == datetime.datetime.fromisoformat(
            "2023-10-01T00:00:00+00:00"
        )

        # It taken_at is not given, then it is set to the current time
        with freeze_time():
            measurement = MeasurementData.model_validate(
                {"quantity_measured": "quality", "value": 25.0}
            )
            assert measurement.taken_at == utc_now()

    def test_hash(self) -> None:
        """Test that MeasurementData is hashable."""
        hash(
            MeasurementData.model_validate(
                {"quantity_measured": "quality", "value": 25.0}
            )
        )
