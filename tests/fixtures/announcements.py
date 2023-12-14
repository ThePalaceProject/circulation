import datetime

import pytest
from sqlalchemy.orm import Session

from core.model import Library
from core.model.announcements import Announcement, AnnouncementData


class AnnouncementFixture:
    """A fixture for tests that need to create announcements."""

    # Create raw data to be used in tests.
    format = "%Y-%m-%d"
    today: datetime.date = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    tomorrow = today + datetime.timedelta(days=1)
    a_week_ago = today - datetime.timedelta(days=7)
    in_a_week = today + datetime.timedelta(days=7)

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
def announcement_fixture() -> AnnouncementFixture:
    return AnnouncementFixture()
