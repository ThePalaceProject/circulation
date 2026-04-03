import datetime

from freezegun import freeze_time

from palace.util.datetime_helpers import utc_now

from palace.manager.data_layer.measurement import MeasurementData


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

        # If taken_at is not given, it defaults to None.
        # The timestamp will be filled in from the parent BibliographicData's
        # as_of_timestamp when the measurement is applied to an edition.
        measurement = MeasurementData.model_validate(
            {"quantity_measured": "quality", "value": 25.0}
        )
        assert measurement.taken_at is None

    def test_hash(self) -> None:
        """Test that MeasurementData is hashable."""
        hash(
            MeasurementData.model_validate(
                {"quantity_measured": "quality", "value": 25.0}
            )
        )
