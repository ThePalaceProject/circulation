from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import Enum as AlchemyEnum
from sqlalchemy import ForeignKey, Integer, Unicode
from sqlalchemy.orm import Mapped, relationship

from core.model import Base

if TYPE_CHECKING:
    from core.model import IntegrationConfiguration, Library


class RegistrationStage(Enum):
    """The stage of a library's registration with a discovery service."""

    TESTING = "testing"
    PRODUCTION = "production"


class RegistrationStatus(Enum):
    """The status of a library's registration with a discovery service."""

    SUCCESS = "success"
    FAILURE = "failure"


class DiscoveryServiceRegistration(Base):
    """A library's registration with a discovery service."""

    __tablename__ = "discovery_service_registrations"

    status = Column(
        AlchemyEnum(RegistrationStatus),
        default=RegistrationStatus.FAILURE,
        nullable=False,
    )
    stage = Column(
        AlchemyEnum(RegistrationStage),
        default=RegistrationStage.TESTING,
        nullable=False,
    )
    web_client = Column(Unicode)

    short_name = Column(Unicode)
    shared_secret = Column(Unicode)

    # The IntegrationConfiguration this registration is associated with.
    integration_id = Column(
        Integer,
        ForeignKey("integration_configurations.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    integration: Mapped[IntegrationConfiguration] = relationship(
        "IntegrationConfiguration"
    )

    # The Library this registration is associated with.
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    library: Mapped[Library] = relationship("Library")

    vendor_id = Column(Unicode)
