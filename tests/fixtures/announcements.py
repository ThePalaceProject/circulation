import datetime

import pytest


class AnnouncementFixture:
    """A fixture for tests that need to create announcements."""

    # Create raw data to be used in tests.
    format = "%Y-%m-%d"
    today_date: datetime.date = datetime.date.today()
    yesterday = (today_date - datetime.timedelta(days=1)).strftime(format)
    tomorrow = (today_date + datetime.timedelta(days=1)).strftime(format)
    a_week_ago = (today_date - datetime.timedelta(days=7)).strftime(format)
    in_a_week = (today_date + datetime.timedelta(days=7)).strftime(format)
    today = today_date.strftime(format)

    # This announcement is active.
    active = dict(
        id="active", start=today, finish=tomorrow, content="A sample announcement."
    )

    # This announcement expired yesterday.
    expired = dict(active)
    expired["id"] = "expired"
    expired["start"] = a_week_ago
    expired["finish"] = yesterday

    # This announcement should be displayed starting tomorrow.
    forthcoming = dict(active)
    forthcoming["id"] = "forthcoming"
    forthcoming["start"] = tomorrow
    forthcoming["finish"] = in_a_week


@pytest.fixture(scope="function")
def announcement_fixture() -> AnnouncementFixture:
    return AnnouncementFixture()
