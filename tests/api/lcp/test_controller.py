import json
from unittest.mock import MagicMock, call, create_autospec, patch

from flask import request

from api.lcp.collection import LCPAPI
from api.lcp.controller import LCPController
from api.lcp.factory import LCPServerFactory
from api.lcp.server import LCPServer
from core.external_search import MockExternalSearchIndex
from core.lcp.credential import LCPCredentialFactory, LCPUnhashedPassphrase
from core.model import ExternalIntegration
from core.model.library import Library
from tests.api.lcp import lcp_strings
from tests.api.mockapi.circulation import MockCirculationAPI, MockCirculationManager
from tests.fixtures.api_controller import ControllerFixture

manager_api_cls = dict(
    circulationapi_cls=MockCirculationAPI,
    externalsearch_cls=MockExternalSearchIndex,
)


class TestLCPController:
    def test_get_lcp_passphrase_returns_the_same_passphrase_for_authenticated_patron(
        self, controller_fixture: ControllerFixture
    ):
        # Arrange
        expected_passphrase = LCPUnhashedPassphrase(
            "1cde00b4-bea9-48fc-819b-bd17c578a22c"
        )

        with patch(
            "api.lcp.controller.LCPCredentialFactory"
        ) as credential_factory_constructor_mock:
            credential_factory = create_autospec(spec=LCPCredentialFactory)
            credential_factory.get_patron_passphrase = MagicMock(
                return_value=expected_passphrase
            )
            credential_factory_constructor_mock.return_value = credential_factory

            patron = controller_fixture.default_patron
            manager = MockCirculationManager(controller_fixture.db.session)
            controller = LCPController(manager)
            controller.authenticated_patron_from_request = MagicMock(  # type: ignore
                return_value=patron
            )

            url = "http://circulationmanager.org/lcp/hint"

            with controller_fixture.app.test_request_context(url):
                request.library: Library = controller_fixture.db.default_library()  # type: ignore

                # Act
                result1 = controller.get_lcp_passphrase()
                result2 = controller.get_lcp_passphrase()

                # Assert
                for result in [result1, result2]:
                    assert result.status_code == 200
                    assert ("passphrase" in result.json) == True
                    assert result.json["passphrase"] == expected_passphrase.text

                credential_factory.get_patron_passphrase.assert_has_calls(
                    [
                        call(controller_fixture.db.session, patron),
                        call(controller_fixture.db.session, patron),
                    ]
                )

    def test_get_lcp_license_returns_problem_detail_when_collection_is_missing(
        self, controller_fixture
    ):
        # Arrange
        missing_collection_name = "missing-collection"
        license_id = "e99be177-4902-426a-9b96-0872ae877e2f"
        expected_license = json.loads(lcp_strings.LCPSERVER_LICENSE)
        lcp_server = create_autospec(spec=LCPServer)
        lcp_server.get_license = MagicMock(return_value=expected_license)
        library = controller_fixture.db.default_library()
        lcp_collection = controller_fixture.db.collection(
            LCPAPI.NAME, ExternalIntegration.LCP
        )
        library.collections.append(lcp_collection)

        with patch(
            "api.lcp.controller.LCPServerFactory"
        ) as lcp_server_factory_constructor_mock:
            lcp_server_factory = create_autospec(spec=LCPServerFactory)
            lcp_server_factory.create = MagicMock(return_value=lcp_server)
            lcp_server_factory_constructor_mock.return_value = lcp_server_factory

            patron = controller_fixture.default_patron
            manager = MockCirculationManager(controller_fixture.db.session)
            controller = LCPController(manager)
            controller.authenticated_patron_from_request = MagicMock(
                return_value=patron
            )

            url = "http://circulationmanager.org/{}/licenses{}".format(
                missing_collection_name, license_id
            )

            with controller_fixture.app.test_request_context(url):
                request.library = controller_fixture.db.default_library()

                # Act
                result = controller.get_lcp_license(missing_collection_name, license_id)

                # Assert
                assert result.status_code == 404

    def test_get_lcp_license_returns_the_same_license_for_authenticated_patron(
        self, controller_fixture
    ):
        # Arrange
        license_id = "e99be177-4902-426a-9b96-0872ae877e2f"
        expected_license = json.loads(lcp_strings.LCPSERVER_LICENSE)
        lcp_server = create_autospec(spec=LCPServer)
        lcp_server.get_license = MagicMock(return_value=expected_license)
        library = controller_fixture.db.default_library()
        lcp_collection = controller_fixture.db.collection(
            LCPAPI.NAME, ExternalIntegration.LCP
        )
        library.collections.append(lcp_collection)

        with patch(
            "api.lcp.controller.LCPServerFactory"
        ) as lcp_server_factory_constructor_mock:
            lcp_server_factory = create_autospec(spec=LCPServerFactory)
            lcp_server_factory.create = MagicMock(return_value=lcp_server)
            lcp_server_factory_constructor_mock.return_value = lcp_server_factory

            patron = controller_fixture.default_patron
            manager = MockCirculationManager(controller_fixture.db.session)
            controller = LCPController(manager)
            controller.authenticated_patron_from_request = MagicMock(
                return_value=patron
            )

            url = "http://circulationmanager.org/{}/licenses{}".format(
                LCPAPI.NAME, license_id
            )

            with controller_fixture.app.test_request_context(url):
                request.library = controller_fixture.db.default_library()

                # Act
                result1 = controller.get_lcp_license(LCPAPI.NAME, license_id)
                result2 = controller.get_lcp_license(LCPAPI.NAME, license_id)

                # Assert
                for result in [result1, result2]:
                    assert result.status_code == 200
                    assert result.json == expected_license
