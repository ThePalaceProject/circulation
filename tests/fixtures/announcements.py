import datetime
from collections.abc import Generator

import pytest
from freezegun import freeze_time
from sqlalchemy.orm import Session

from palace.manager.sqlalchemy.model.announcements import Announcement, AnnouncementData
from palace.manager.sqlalchemy.model.library import Library


class AnnouncementFixture:
    """A fixture for tests that need to create announcements."""

    format = "%Y-%m-%d"

    def __init__(self) -> None:
        # Capture the dates relative to "today" when the fixture is
        # instantiated. The ``announcement_fixture`` below freezes the
        # clock for the duration of the test, so these match the
        # ``datetime.date.today()`` calls the model and validator make at
        # runtime -- without that, a test running across the midnight UTC
        # boundary would compute its expectations against one date and the
        # code under test against the next.
        self.today = datetime.date.today()
        self.yesterday = self.today - datetime.timedelta(days=1)
        self.tomorrow = self.today + datetime.timedelta(days=1)
        self.a_week_ago = self.today - datetime.timedelta(days=7)
        self.in_a_week = self.today + datetime.timedelta(days=7)

    def create_announcement(
        self,
        db: Session,
        start: datetime.date | None = None,
        finish: datetime.date | None = None,
        content: str = "test",
        library: Library | None = None,
    ) -> Announcement:
        if start is None:
            start = self.today
        if finish is None:
            finish = self.today + datetime.timedelta(days=1)
        data = AnnouncementData(
            content=content,
            start=start,
            finish=finish,
        )
        announcement = Announcement.from_data(db, data, library)
        return announcement

    def active_announcement(
        self, db: Session, library: Library | None = None
    ) -> Announcement:
        # This announcement is active.
        return self.create_announcement(
            db,
            start=self.today,
            finish=self.tomorrow,
            content="active",
            library=library,
        )

    def expired_announcement(
        self, db: Session, library: Library | None = None
    ) -> Announcement:
        # This announcement expired yesterday.
        return self.create_announcement(
            db,
            start=self.a_week_ago,
            finish=self.yesterday,
            content="expired",
            library=library,
        )

    def forthcoming_announcement(
        self, db: Session, library: Library | None = None
    ) -> Announcement:
        # This announcement should be displayed starting tomorrow.
        return self.create_announcement(
            db,
            start=self.tomorrow,
            finish=self.in_a_week,
            content="forthcoming",
            library=library,
        )


@pytest.fixture(scope="function")
def announcement_fixture() -> Generator[AnnouncementFixture]:
    # Freeze the clock so the fixture's dates and the runtime
    # ``datetime.date.today()`` calls in the model and validator agree,
    # even when the test runs across the midnight UTC boundary.
    with freeze_time():
        yield AnnouncementFixture()
