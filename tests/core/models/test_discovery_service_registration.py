import pytest
from sqlalchemy import select

from core.integration.goals import Goals
from core.model import IntegrationConfiguration, Library, create
from core.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStage,
    RegistrationStatus,
)
from tests.fixtures.database import (
    DatabaseTransactionFixture,
    IntegrationConfigurationFixture,
)
from tests.fixtures.library import LibraryFixture


class RegistrationFixture:
    def __call__(
        self,
        library: Library | None = None,
        integration: IntegrationConfiguration | None = None,
    ) -> DiscoveryServiceRegistration:
        library = library or self.library_fixture.library()
        integration = integration or self.integration_fixture(
            "test", Goals.DISCOVERY_GOAL
        )
        registration, _ = create(
            self.db.session,
            DiscoveryServiceRegistration,
            library=library,
            integration=integration,
        )
        return registration

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
        integration_fixture: IntegrationConfigurationFixture,
    ) -> None:
        self.db = db
        self.library_fixture = library_fixture
        self.integration_fixture = integration_fixture


@pytest.fixture
def registration_fixture(
    db: DatabaseTransactionFixture,
    library_fixture: LibraryFixture,
    create_integration_configuration: IntegrationConfigurationFixture,
) -> RegistrationFixture:
    return RegistrationFixture(db, library_fixture, create_integration_configuration)


class TestDiscoveryServiceRegistration:
    def test_constructor(self, registration_fixture: RegistrationFixture):
        registration = registration_fixture()

        # We get default values for status and stage.
        assert registration.status == RegistrationStatus.FAILURE
        assert registration.stage == RegistrationStage.TESTING

        assert registration.web_client is None

    @pytest.mark.parametrize(
        "parent",
        [
            "library",
            "integration",
        ],
    )
    def test_registration_deleted_when_parent_deleted(
        self,
        db: DatabaseTransactionFixture,
        registration_fixture: RegistrationFixture,
        parent: str,
    ):
        registration = registration_fixture()

        registrations = db.session.execute(select(DiscoveryServiceRegistration)).all()
        assert len(registrations) == 1

        parent = getattr(registration, parent)
        db.session.delete(parent)
        db.session.flush()

        registrations = db.session.execute(select(DiscoveryServiceRegistration)).all()
        assert len(registrations) == 0
