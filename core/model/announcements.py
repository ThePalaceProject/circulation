from __future__ import annotations

import dataclasses
import datetime
import uuid
from typing import TYPE_CHECKING, Dict, List, Optional

from sqlalchemy import Column, Date, ForeignKey, Integer, Unicode, select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, Session, relationship
from sqlalchemy.sql import Select

from core.model import Base, create

if TYPE_CHECKING:
    from core.model import Library

SETTING_NAME = "announcements"


class Announcement(Base):
    """Sqlalchemy model for an announcement."""

    __tablename__ = "announcements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    content = Column(Unicode, nullable=False)
    start = Column(Date, nullable=False)
    finish = Column(Date, nullable=False)

    # The Library associated with the announcement, announcements that should be shown to
    # all libraries will have a null library_id.
    library_id = Column(
        Integer,
        ForeignKey("libraries.id"),
        index=True,
        nullable=True,
    )

    library: Mapped[Library] = relationship(
        "Library", back_populates="library_announcements"
    )

    @classmethod
    def global_announcements(cls) -> Select:
        return select(cls).where(cls.library_id == None).order_by(cls.start)

    @classmethod
    def library_announcements(cls, library: Library) -> Select:
        return select(cls).where(cls.library_id == library.id).order_by(cls.start)

    @classmethod
    def authentication_document_announcements(
        cls, library: Library
    ) -> List[Dict[str, str]]:
        db = Session.object_session(library)
        today_local = datetime.date.today()
        query = (
            select(cls.id, cls.content)
            .where((cls.library_id == library.id) | (cls.library_id == None))
            .where(cls.start <= today_local)
            .where(cls.finish >= today_local)
            .order_by(cls.library_id.desc(), cls.start)
        )
        return [
            {"id": str(id), "content": str(content)}
            for id, content in db.execute(query)
        ]

    @classmethod
    def from_data(
        cls, db: Session, data: AnnouncementData, library: Optional[Library] = None
    ) -> Announcement:
        created, _ = create(
            db,
            cls,
            id=data.id,
            content=data.content,
            start=data.start,
            finish=data.finish,
            library=library,
        )
        return created

    @classmethod
    def sync(
        cls,
        db: Session,
        existing: List[Announcement],
        new: Dict[uuid.UUID, AnnouncementData],
        library: Optional[Library] = None,
    ) -> None:
        """
        Synchronize the existing announcements with the new announcements, creating any new announcements
        and updating or deleting existing announcements as necessary.

        After this function is run the database will contain the same announcements as the new dict.
        """
        existing_announcements = {x.id: x for x in existing}
        announcements_to_delete = existing_announcements.keys() - new.keys()
        announcements_to_update = existing_announcements.keys() & new.keys()
        announcements_to_create = new.keys() - existing_announcements.keys()

        for id in announcements_to_delete:
            db.delete(existing_announcements[id])

        for id in announcements_to_update:
            existing_announcements[id].update(new[id])  # type: ignore[index]

        for id in announcements_to_create:
            Announcement.from_data(db, new[id], library=library)

    def update(self, data: AnnouncementData) -> None:
        if data.id is not None and data.id != self.id:
            raise ValueError(
                f"Cannot change announcement id from {self.id} to {data.id}"
            )
        self.content = data.content
        self.start = data.start
        self.finish = data.finish

    def to_data(self) -> AnnouncementData:
        assert self.id is not None
        assert self.content is not None
        assert self.start is not None
        assert self.finish is not None
        return AnnouncementData(
            id=self.id,
            content=self.content,
            start=self.start,
            finish=self.finish,
        )

    def __repr__(self) -> str:
        return f"<Announcement {self.id} dates={self.start}-{self.finish} library={self.library_id} {self.content}>"


@dataclasses.dataclass
class AnnouncementData:
    content: str
    start: datetime.date
    finish: datetime.date
    id: Optional[uuid.UUID] = None

    def as_dict(self) -> Dict[str, str]:
        date_format = "%Y-%m-%d"
        return_dict = {
            "content": self.content,
            "start": self.start.strftime(date_format),
            "finish": self.finish.strftime(date_format),
        }
        if self.id is not None:
            return_dict["id"] = str(self.id)
        return return_dict
