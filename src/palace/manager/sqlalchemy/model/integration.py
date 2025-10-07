from __future__ import annotations

from pprint import pformat
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Column,
    Enum as SQLAlchemyEnum,
    ForeignKey,
    Index,
    Integer,
    Unicode,
    cast,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, Query, Session, relationship

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.base import Base

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection
    from palace.manager.sqlalchemy.model.library import Library


class IntegrationConfiguration(Base):
    """
    Integration Configuration

    It stores the configuration settings for each integration in
    a single json row in the database. These settings are then serialized
    using Pydantic to a python object.
    """

    __tablename__ = "integration_configurations"
    id: Mapped[int] = Column(Integer, primary_key=True)

    # The protocol is used to load the correct implementation class for
    # this integration. It is looked up in the IntegrationRegistry.
    protocol: Mapped[str] = Column(Unicode, nullable=False)

    # The goal of the integration is used to differentiate between the
    # different types of integrations. For example, a goal of "authentication"
    # would be used for an authentication provider.
    goal: Mapped[Goals] = Column(SQLAlchemyEnum(Goals), nullable=False, index=True)

    # A unique name for this integration. This is primarily
    # used to identify integrations from command-line scripts.
    name: Mapped[str] = Column(Unicode, nullable=False, unique=True)

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
        """
        Update the context for this integration using an atomic database operation.

        This method uses PostgreSQL's JSONB concatenation operator to atomically
        merge new_context into the existing context at the database level,
        preventing race conditions when multiple processes update the context
        simultaneously.

        :param new_context: Dictionary of key-value pairs to merge into the context
        :raises PalaceValueError: If the object is not bound to a database session
        """
        db = Session.object_session(self)
        if db is None:
            raise PalaceValueError("Object is not bound to a session")

        # Use PostgreSQL's JSONB concatenation operator (||) for atomic update.
        # This performs the merge at the database level, preventing lost updates
        # when multiple processes modify different keys concurrently.
        stmt = (
            update(IntegrationConfiguration)
            .where(IntegrationConfiguration.id == self.id)
            .values(
                context=IntegrationConfiguration.context.op("||")(
                    cast(new_context, JSONB)
                )
            )
        )
        db.execute(stmt)

        # Refresh the object to get the updated context from the database
        db.refresh(self)

    # Self test results, stored as json.
    self_test_results: Mapped[dict[str, Any]] = Column(
        JSONB, nullable=False, default=dict
    )

    library_configurations: Mapped[list[IntegrationLibraryConfiguration]] = (
        relationship(
            "IntegrationLibraryConfiguration",
            back_populates="parent",
            uselist=True,
            cascade="all, delete-orphan",
            passive_deletes=True,
        )
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
        from palace.manager.sqlalchemy.model.library import Library

        if library is None:
            return None

        db = Session.object_session(self)
        if isinstance(library, Library):
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

    def explain(self, include_secrets: bool = False) -> list[str]:
        """Create a series of human-readable strings to explain an
        Integrations's settings.
        """
        lines = []
        lines.append(f"ID: {self.id}")
        lines.append(f"Name: {self.name}")
        lines.append(f"Protocol/Goal: {self.protocol}/{self.goal}")

        def process_settings_dict(
            settings_dict: dict[str, Any], indent: int = 0
        ) -> None:
            secret_keys = ["key", "password", "token", "secret"]
            for setting_key, setting_value in sorted(settings_dict.items()):
                if (
                    any(secret_key in setting_key for secret_key in secret_keys)
                    and not include_secrets
                ):
                    setting_value = "********"
                lines.append(" " * indent + f"{setting_key}: {setting_value}")

        if len(self.settings_dict) > 0:
            lines.append("Settings:")
            process_settings_dict(self.settings_dict, 2)

        if len(self.context) > 0:
            lines.append("Context:")
            process_settings_dict(self.context, 2)

        if isinstance(self.self_test_results, dict) and len(self.self_test_results) > 0:
            lines.append("Self Test Results:")
            lines.append(pformat(self.self_test_results, indent=2))

        if len(self.library_configurations) > 0:
            lines.append("Configured libraries:")
            for library_configuration in self.library_configurations:
                lines.append(
                    f"  {library_configuration.library.short_name} - {library_configuration.library.name}"
                )
                if (
                    isinstance(library_configuration.settings_dict, dict)
                    and len(library_configuration.settings_dict) > 0
                ):
                    lines.append("    Settings:")
                    process_settings_dict(library_configuration.settings_dict, 6)

        return lines


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
    parent_id: Mapped[int] = Column(
        Integer,
        ForeignKey("integration_configurations.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    parent: Mapped[IntegrationConfiguration] = relationship(
        "IntegrationConfiguration", back_populates="library_configurations"
    )

    # The library this integration is associated with.
    library_id: Mapped[int] = Column(
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
