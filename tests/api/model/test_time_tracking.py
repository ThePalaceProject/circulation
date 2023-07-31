import pytest

from api.model.time_tracking import PlaytimeTimeEntry


class TestPlaytimeEntriesModels:
    def test_validations(self):
        # Timezone validation
        with pytest.raises(ValueError) as raised:
            PlaytimeTimeEntry(
                id="", during_minute="2000-01-01T12:00:00+01:00", seconds_played=12
            )
        assert raised.value.errors()[0]["msg"] == "Timezone MUST be UTC always"

        # Seconds played coercion
        entry2 = PlaytimeTimeEntry(
            id="", during_minute="2000-01-01T12:00:00+00:00", seconds_played=45384
        )
        assert entry2.seconds_played == 60

        entry2 = PlaytimeTimeEntry(
            id="", during_minute="2000-01-01T12:00:00+00:00", seconds_played=-45384
        )
        assert entry2.seconds_played == 0

        # Minute boundary coercion
        entry2 = PlaytimeTimeEntry(
            id="", during_minute="2000-01-01T12:00:06.123456+00:00", seconds_played=12
        )
        assert entry2.during_minute.isoformat() == "2000-01-01T12:00:00+00:00"
