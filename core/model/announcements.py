import uuid

from sqlalchemy import Column, Date, ForeignKey, Integer, Unicode
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, relationship

from core.model import Base, Library


class Announcement(Base):

    __tablename__ = "announcements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    content = Column(Unicode)
    start = Column(Date)
    finish = Column(Date)

    # The Library associated with the announcement, announcements that should be shown to
    # all libraries will have a null library_id.
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    library: Mapped[Library] = relationship("Library")
