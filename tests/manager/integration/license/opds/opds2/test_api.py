from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.api.circulation.fulfillment import Fulfillment, RedirectFulfillment
from palace.manager.celery.tasks import opds2 as opds2_celery
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLSubject,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.http import MockHttpClientFixture


class Opds2ApiFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, http_client: MockHttpClientFixture
    ):
        self.patron = db.patron()
        self.collection: Collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds_settings(
                external_account_id="http://opds2.example.org/feed",
                data_source="test",
            ),
        )
        self.collection.integration_configuration.context = {
            OPDS2API.TOKEN_AUTH_CONFIG_KEY: "http://example.org/token?userName={patron_id}"
        }

        self.http_client = http_client
        self.data_source = DataSource.lookup(db.session, "test", autocreate=True)

        self.pool = MagicMock(spec=LicensePool)
        self.mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
        self.pool.available_delivery_mechanisms = [self.mechanism]
        self.pool.data_source = self.data_source
        self.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill?authToken={authentication_token}"
        )

        self.api = OPDS2API(db.session, self.collection)

    def queue_default_auth_token_response(self) -> None:
        """Queue a successful authentication token response."""
        self.http_client.queue_response(200, content="plaintext-auth-token")

    def fulfill(self) -> Fulfillment:
        return self.api.fulfill(self.patron, "", self.pool, self.mechanism)


@pytest.fixture
def opds2_api_fixture(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
) -> Generator[Opds2ApiFixture, None, None]:
    yield Opds2ApiFixture(db, http_client)


class TestOpds2Api:
    def test_token_fulfill(self, opds2_api_fixture: Opds2ApiFixture):
        opds2_api_fixture.queue_default_auth_token_response()
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)

        patron_id = opds2_api_fixture.patron.identifier_to_remote_service(
            opds2_api_fixture.data_source
        )

        assert len(opds2_api_fixture.http_client.requests) == 1
        assert (
            opds2_api_fixture.http_client.requests[0]
            == f"http://example.org/token?userName={patron_id}"
        )

        assert (
            fulfillment.content_link
            == "http://example.org/11234/fulfill?authToken=plaintext-auth-token"
        )

    def test_token_fulfill_alternate_template(self, opds2_api_fixture: Opds2ApiFixture):
        # Alternative templating
        opds2_api_fixture.queue_default_auth_token_response()
        opds2_api_fixture.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill{?authentication_token}"
        )
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)

        assert (
            fulfillment.content_link
            == "http://example.org/11234/fulfill?authentication_token=plaintext-auth-token"
        )

    def test_token_fulfill_400_response(self, opds2_api_fixture: Opds2ApiFixture):
        # non-200 response
        opds2_api_fixture.http_client.queue_response(400, content="error")
        with pytest.raises(CannotFulfill):
            opds2_api_fixture.fulfill()

    def test_token_fulfill_no_template(self, opds2_api_fixture: Opds2ApiFixture):
        # No templating in the url
        opds2_api_fixture.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill"
        )
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)
        assert (
            fulfillment.content_link
            == opds2_api_fixture.mechanism.resource.representation.public_url
        )

    def test_token_fulfill_no_endpoint_config(self, opds2_api_fixture: Opds2ApiFixture):
        # No token endpoint config
        opds2_api_fixture.api.token_auth_configuration = None
        mock = MagicMock()
        opds2_api_fixture.api.fulfill_token_auth = mock
        opds2_api_fixture.fulfill()
        # we never call the token auth function
        assert mock.call_count == 0

    def test_get_authentication_token(self, opds2_api_fixture: Opds2ApiFixture):
        opds2_api_fixture.queue_default_auth_token_response()
        token = opds2_api_fixture.api.get_authentication_token(
            opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
        )

        assert token == "plaintext-auth-token"
        assert len(opds2_api_fixture.http_client.requests) == 1

    def test_get_authentication_token_400_response(
        self, opds2_api_fixture: Opds2ApiFixture, caplog: pytest.LogCaptureFixture
    ):
        opds2_api_fixture.http_client.queue_response(400, content="error")
        with pytest.raises(CannotFulfill):
            opds2_api_fixture.api.get_authentication_token(
                opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
            )

        # Verify detailed error message is logged
        assert "Could not authenticate the patron" in caplog.text
        assert "Bad status code 400" in caplog.text
        assert "expected 2xx" in caplog.text

    def test_get_authentication_token_bad_response(
        self, opds2_api_fixture: Opds2ApiFixture, caplog: pytest.LogCaptureFixture
    ):
        opds2_api_fixture.http_client.queue_response(200, content="")
        with pytest.raises(CannotFulfill):
            opds2_api_fixture.api.get_authentication_token(
                opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
            )

        # Verify detailed error message is logged
        assert "Could not authenticate the patron" in caplog.text
        assert "Empty response from" in caplog.text
        assert "expected an authentication token" in caplog.text

    def test_import_task(self) -> None:
        collection_id = MagicMock()
        force = MagicMock()
        with patch.object(opds2_celery, "import_collection") as mock_import:
            result = OPDS2API.import_task(collection_id, force)

        mock_import.s.assert_called_once_with(collection_id, force=force)
        assert result == mock_import.s.return_value

    def test_get_authentication_token_with_saml_parameters(
        self, opds2_api_fixture: Opds2ApiFixture
    ):
        """Test get_authentication_token expands SAML parameters in URL templates"""
        # Create SAML credentials with full data (entity ID and affiliation)
        attributes = [
            SAMLAttribute(
                name=SAMLAttributeType.eduPersonScopedAffiliation.name,
                values=["faculty@example.edu", "member@example.edu"],
            )
        ]
        attribute_statement = SAMLAttributeStatement(attributes)
        saml_subject = SAMLSubject(
            idp="https://idp.example.org/shibboleth",
            name_id=None,
            attribute_statement=attribute_statement,
        )

        # Create credential for the patron
        opds2_api_fixture.api.saml_credential_manager.create_saml_token(
            opds2_api_fixture.api._db, opds2_api_fixture.patron, saml_subject
        )

        # Test 1: Template with only saml_entity_id
        opds2_api_fixture.queue_default_auth_token_response()
        token = opds2_api_fixture.api.get_authentication_token(
            opds2_api_fixture.patron,
            opds2_api_fixture.data_source,
            "http://example.org/token?idp={saml_entity_id}",
        )
        assert token == "plaintext-auth-token"
        called_url = opds2_api_fixture.http_client.requests[0]
        assert (
            called_url
            == "http://example.org/token?idp=https%3A%2F%2Fidp.example.org%2Fshibboleth"
        )

        # Test 2: Template with both saml_entity_id and saml_person_scoped_affiliation
        opds2_api_fixture.http_client.requests.clear()
        opds2_api_fixture.queue_default_auth_token_response()
        token = opds2_api_fixture.api.get_authentication_token(
            opds2_api_fixture.patron,
            opds2_api_fixture.data_source,
            "http://example.org/token?idp={saml_entity_id}&affiliation={saml_person_scoped_affiliation}",
        )
        assert token == "plaintext-auth-token"
        called_url = opds2_api_fixture.http_client.requests[0]
        assert (
            called_url
            == "http://example.org/token?idp=https%3A%2F%2Fidp.example.org%2Fshibboleth&affiliation=faculty%40example.edu,member%40example.edu"
        )

    @pytest.mark.parametrize(
        "template_url,saml_subject,expected_missing",
        [
            pytest.param(
                "http://example.org/token?idp={saml_entity_id}",
                None,
                ["saml_entity_id"],
                id="no_credentials_requires_entity_id",
            ),
            pytest.param(
                "http://example.org/token?aff={saml_person_scoped_affiliation}",
                None,
                ["saml_person_scoped_affiliation"],
                id="no_credentials_requires_affiliation",
            ),
            pytest.param(
                "http://example.org/token?idp={saml_entity_id}&aff={saml_person_scoped_affiliation}",
                None,
                ["saml_entity_id", "saml_person_scoped_affiliation"],
                id="no_credentials_requires_both",
            ),
            pytest.param(
                "http://example.org/token?idp={saml_entity_id}&aff={saml_person_scoped_affiliation}",
                SAMLSubject(
                    idp="https://idp.example.org/shibboleth",
                    name_id=None,
                    attribute_statement=None,
                ),
                ["saml_person_scoped_affiliation"],
                id="has_entity_id_only_requires_both",
            ),
            pytest.param(
                "http://example.org/token?aff={saml_person_scoped_affiliation}",
                SAMLSubject(
                    idp="https://idp.example.org/shibboleth",
                    name_id=None,
                    attribute_statement=SAMLAttributeStatement(
                        [
                            SAMLAttribute(
                                name=SAMLAttributeType.givenName.name,
                                values=["John"],
                            )
                        ]
                    ),
                ),
                ["saml_person_scoped_affiliation"],
                id="has_other_attributes_but_missing_affiliation",
            ),
        ],
    )
    def test_get_authentication_token_missing_saml_credentials(
        self,
        opds2_api_fixture: Opds2ApiFixture,
        caplog: pytest.LogCaptureFixture,
        template_url: str,
        saml_subject: SAMLSubject | None,
        expected_missing: list[str],
    ):
        """Test that we fail when template requires SAML params but patron lacks them"""
        # Set up SAML credentials if provided
        if saml_subject:
            opds2_api_fixture.api.saml_credential_manager.create_saml_token(
                opds2_api_fixture.api._db, opds2_api_fixture.patron, saml_subject
            )

        # Attempt to get token should fail
        with pytest.raises(CannotFulfill):
            opds2_api_fixture.api.get_authentication_token(
                opds2_api_fixture.patron,
                opds2_api_fixture.data_source,
                template_url,
            )

        # Verify the error was logged with details about what's missing
        assert "Template requires SAML parameters" in caplog.text
        assert "is missing:" in caplog.text
        # Check each expected missing variable appears in the log (order may vary)
        for missing_var in expected_missing:
            assert missing_var in caplog.text

    def test_get_authentication_token_no_patron_id_in_template(
        self, opds2_api_fixture: Opds2ApiFixture
    ):
        """Test that we don't fetch patron_id if template doesn't need it"""
        # Queue the response
        opds2_api_fixture.queue_default_auth_token_response()

        # Mock identifier_to_remote_service to ensure it's not called
        with patch.object(
            opds2_api_fixture.patron, "identifier_to_remote_service"
        ) as mock_identifier:
            # Template has no variables, so no patron_id lookup should occur
            token = opds2_api_fixture.api.get_authentication_token(
                opds2_api_fixture.patron,
                opds2_api_fixture.data_source,
                "http://example.org/token?key=value",
            )

            # Verify identifier_to_remote_service was NOT called
            mock_identifier.assert_not_called()

        assert token == "plaintext-auth-token"
        assert len(opds2_api_fixture.http_client.requests) == 1

        # Verify the URL was called without patron_id
        called_url = opds2_api_fixture.http_client.requests[0]
        assert called_url == "http://example.org/token?key=value"
