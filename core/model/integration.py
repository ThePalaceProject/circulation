from __future__ import annotations

import sys
from typing import TYPE_CHECKING, List, Type

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import ForeignKey, Integer, Unicode
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, Query, Session, relationship

from core.integration.goals import Goals
from core.integration.status import Status
from core.model import Base, create
from core.util.datetime_helpers import utc_now

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from core.model import Library


class IntegrationConfiguration(Base):
    """
    Integration Configuration

    This is used to store the configuration of integrations. It is
    a combination of the now deprecated ExternalIntegration and
    ConfigutationSetting classes.

    It stores the configuration settings for each external integration in
    a single json row in the database. These settings are then serialized
    using Pydantic to a python object.
    """

    __tablename__ = "integration_configurations"
    id = Column(Integer, primary_key=True)

    # The protocol is used to load the correct implementation class for
    # this integration. It is looked up in the IntegrationRegistry.
    protocol = Column(Unicode, nullable=False)

    # The goal of the integration is used to differentiate between the
    # different types of integrations. For example, a goal of "authentication"
    # would be used for an authentication provider.
    goal = Column(SQLAlchemyEnum(Goals), nullable=False, index=True)

    # A unique name for this ExternalIntegration. This is primarily
    # used to identify ExternalIntegrations from command-line scripts.
    name = Column(Unicode, nullable=False, unique=True)

    # The configuration settings for this integration. Stored as json.
    settings = Column(JSONB, nullable=False, default=dict)

    # Self test results, stored as json.
    self_test_results = Column(JSONB, nullable=False, default=dict)

    # Status
    status = Column(SQLAlchemyEnum(Status), nullable=False, default=Status.GREEN)
    last_status_update = Column(DateTime, nullable=True)

    library_configurations: Mapped[
        List[IntegrationLibraryConfiguration]
    ] = relationship(
        "IntegrationLibraryConfiguration",
        back_populates="parent",
        uselist=True,
        cascade="all, delete",
        passive_deletes=True,
    )

    @property
    def available(self) -> bool:
        return self.status != Status.RED

    def __repr__(self) -> str:
        return f"<IntegrationConfiguration: {self.name} {self.protocol} {self.goal}>"


class IntegrationLibraryConfiguration(Base):
    """
    Integration Library Configuration

    This is used to store the configuration of external integrations that is
    specific for a particular library.

    It stores the configuration settings for each external integration in
    a single json row in the database. These settings are then serialized
    using Pydantic to a python object.
    """

    __tablename__ = "integration_library_configurations"

    # The IntegrationConfiguration this library configuration is
    # associated with.
    parent_id = Column(
        Integer,
        ForeignKey("integration_configurations.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    parent: Mapped[IntegrationConfiguration] = relationship(
        "IntegrationConfiguration", back_populates="library_configurations"
    )

    # The library this integration is associated with. This is optional
    # and is only used for integrations that are specific to a library.
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    library: Mapped[Library] = relationship("Library")

    # The configuration settings for this integration. Stored as json.
    settings = Column(JSONB, nullable=False, default=dict)

    def __repr__(self) -> str:
        return (
            "<IntegrationLibraryConfiguration: "
            f"{self.parent.name} "
            f"{self.library.short_name}>"
        )

    @classmethod
    def for_library_and_goal(
        cls, _db: Session, library: Library, goal: Goals
    ) -> Query[IntegrationLibraryConfiguration]:
        """Get the library configuration for the given library and goal"""
        return (
            _db.query(IntegrationLibraryConfiguration)
            .join(IntegrationConfiguration)
            .filter(
                IntegrationConfiguration.goal == goal,
                IntegrationLibraryConfiguration.library_id == library.id,
            )
        )


class IntegrationError(Base):
    __tablename__ = "integration_errors"

    id = Column(Integer, primary_key=True)
    time = Column(DateTime, default=utc_now)
    error = Column(Unicode)
    integration_id = Column(
        Integer,
        ForeignKey(
            "integration_configurations.id",
            name="fk_integration_error_integration_id",
            ondelete="CASCADE",
        ),
    )

    @classmethod
    def record_error(
        cls: Type[Self],
        _db: Session,
        integration: IntegrationConfiguration,
        error: Exception,
    ) -> Self:
        record, _ = create(
            _db,
            IntegrationError,
            integration_id=integration.id,
            time=utc_now(),
            error=str(error),
        )
        return record  # type: ignore[no-any-return]
