from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import Column
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import ForeignKey, Index, Integer, Unicode, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, Query, Session, relationship
from sqlalchemy.orm.attributes import flag_modified

from core.integration.goals import Goals
from core.model import Base

if TYPE_CHECKING:
    from core.model import Collection, Library


class IntegrationConfiguration(Base):
    """
    Integration Configuration

    This is used to store the configuration of integrations. It is
    a combination of the now deprecated ExternalIntegration and
    ConfigurationSetting classes.

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
    settings_dict: Mapped[dict[str, Any]] = Column(
        "settings", JSONB, nullable=False, default=dict
    )

    # Integration specific context data. Stored as json. This is used to
    # store configuration data that is not user supplied for a particular
    # integration.
    context: Mapped[dict[str, Any]] = Column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        Index(
            "ix_integration_configurations_settings_dict",
            settings_dict,
            postgresql_using="gin",
        ),
    )

    def context_update(self, new_context: dict[str, Any]) -> None:
        """Update the context for this integration"""
        self.context.update(new_context)
        flag_modified(self, "context")

    # Self test results, stored as json.
    self_test_results = Column(JSONB, nullable=False, default=dict)

    library_configurations: Mapped[
        list[IntegrationLibraryConfiguration]
    ] = relationship(
        "IntegrationLibraryConfiguration",
        back_populates="parent",
        uselist=True,
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    collection: Mapped[Collection] = relationship(
        "Collection", back_populates="integration_configuration", uselist=False
    )

    # https://docs.sqlalchemy.org/en/14/orm/extensions/associationproxy.html#simplifying-association-objects
    libraries: Mapped[list[Library]] = association_proxy(
        "library_configurations",
        "library",
        creator=lambda library: IntegrationLibraryConfiguration(library=library),
    )

    def for_library(
        self, library: int | Library | None
    ) -> IntegrationLibraryConfiguration | None:
        """Fetch the library configuration for a specific library"""
        from core.model import Library

        if library is None:
            return None

        db = Session.object_session(self)
        if isinstance(library, Library):
            if library.id is None:
                return None
            library_id = library.id
        else:
            library_id = library

        return db.execute(
            select(IntegrationLibraryConfiguration).where(
                IntegrationLibraryConfiguration.library_id == library_id,
                IntegrationLibraryConfiguration.parent_id == self.id,
            )
        ).scalar_one_or_none()

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

    This is a many-to-many relationship between IntegrationConfiguration and
    Library. Implementing the Association Object pattern:
    https://docs.sqlalchemy.org/en/14/orm/basic_relationships.html#association-object
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

    # The library this integration is associated with.
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    library: Mapped[Library] = relationship("Library")

    # The configuration settings for this integration. Stored as json.
    settings_dict: Mapped[dict[str, Any]] = Column(
        "settings", JSONB, nullable=False, default=dict
    )

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
