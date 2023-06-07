from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from sqlalchemy import Column
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import ForeignKey, Integer, Unicode
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, Query, Session, relationship

from core.integration.goals import Goals
from core.model import Base, create, get_one_or_create
from core.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from core.model import Collection, Library


class SettingsModel:
    """A dict like interface for any item with a settings property"""

    def set(self, key: str, value: Any) -> None:
        """Setting any value in the settings dict can only
        be signalled for a flush to the DB if the dict object has changed.
        This is an SALAlchemy idiosyncrasy."""
        settings = self.settings.copy()
        settings[key] = value
        self.settings = settings

    def get(self, key: str, *args) -> Any:
        return self.settings.get(key, *args)

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def __getitem__(self, key: str) -> Any:
        self.settings[key]


class IntegrationConfiguration(Base, SettingsModel):
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
    settings = Column(JSONB, nullable=False, default=dict)

    # Self test results, stored as json.
    self_test_results = Column(JSONB, nullable=False, default=dict)

    library_configurations: Mapped[
        List[IntegrationLibraryConfiguration]
    ] = relationship(
        "IntegrationLibraryConfiguration",
        back_populates="parent",
        uselist=True,
        cascade="all, delete",
        passive_deletes=True,
    )

    collection: Mapped[Collection] = relationship("Collection", uselist=False)

    def for_library(
        self, library_id: int, create: bool = False
    ) -> IntegrationLibraryConfiguration | None:
        """Fetch the library configuration specifically by library_id"""
        for config in self.library_configurations:
            if config.library_id == library_id:
                return config
        if create:
            session = Session.object_session(self)
            config, _ = get_one_or_create(
                session,
                IntegrationLibraryConfiguration,
                parent_id=self.id,
                library_id=library_id,
            )
            return config
        return None

    def __repr__(self) -> str:
        return f"<IntegrationConfiguration: {self.name} {self.protocol} {self.goal}>"


class IntegrationLibraryConfiguration(Base, SettingsModel):
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
