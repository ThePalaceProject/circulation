from freezegun import freeze_time

from palace.manager.service.celery.celery import add_enqueued_at_header


class TestAddEnqueuedAtHeader:
    def test_sets_iso_timestamp_when_missing(self):
        headers: dict = {}
        with freeze_time("2026-05-13T12:00:00+00:00"):
            add_enqueued_at_header(headers)
        assert headers["enqueued_at"] == "2026-05-13T12:00:00+00:00"

    def test_preserves_existing_value(self):
        # Idempotent across retries / chained re-publishes: keep the original enqueue time.
        headers: dict = {"enqueued_at": "2026-01-01T00:00:00+00:00"}
        with freeze_time("2026-05-13T12:00:00+00:00"):
            add_enqueued_at_header(headers)
        assert headers["enqueued_at"] == "2026-01-01T00:00:00+00:00"
