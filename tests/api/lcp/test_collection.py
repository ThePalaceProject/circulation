import datetime
import json
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from freezegun import freeze_time

from api.lcp.collection import LCPAPI, LCPFulfilmentInfo
from api.lcp.server import LCPServer, LCPServerConstants
from core.model import DataSource, ExternalIntegration
from core.model.configuration import HasExternalIntegration
from core.util.datetime_helpers import utc_now
from tests.api.lcp import lcp_strings
from tests.fixtures.database import DatabaseTransactionFixture


class LCPAPIFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.lcp_collection = self.db.collection(protocol=ExternalIntegration.LCP)
        self.integration = self.lcp_collection.external_integration

        integration_association = create_autospec(spec=HasExternalIntegration)
        integration_association.external_integration = MagicMock(
            return_value=self.integration
        )


@pytest.fixture(scope="function")
def lcp_api_fixture(db: DatabaseTransactionFixture) -> LCPAPIFixture:
    return LCPAPIFixture(db)


class TestLCPAPI:
    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_without_existing_loan(self, lcp_api_fixture):
        # Arrange
        lcp_api = LCPAPI(lcp_api_fixture.db.session, lcp_api_fixture.lcp_collection)
        patron = lcp_api_fixture.db.patron()
        days = lcp_api_fixture.lcp_collection.default_loan_period(patron.library)
        start_date = utc_now()
        end_date = start_date + datetime.timedelta(days=days)
        data_source = DataSource.lookup(
            lcp_api_fixture.db.session, DataSource.LCP, autocreate=True
        )
        data_source_name = data_source.name
        edition = lcp_api_fixture.db.edition(
            data_source_name=data_source_name, identifier_id=lcp_strings.CONTENT_ID
        )
        license_pool = lcp_api_fixture.db.licensepool(
            edition=edition,
            data_source_name=data_source_name,
            collection=lcp_api_fixture.lcp_collection,
        )
        lcp_license = json.loads(lcp_strings.LCPSERVER_LICENSE)
        lcp_server_mock = create_autospec(spec=LCPServer)
        lcp_server_mock.generate_license = MagicMock(return_value=lcp_license)

        configuration = lcp_api_fixture.lcp_collection.integration_configuration

        with patch("api.lcp.collection.LCPServer") as lcp_server_constructor:
            lcp_server_constructor.return_value = lcp_server_mock

            configuration["lcpserver_url"] = lcp_strings.LCPSERVER_URL
            configuration["lcpserver_user"] = lcp_strings.LCPSERVER_USER
            configuration["lcpserver_password"] = lcp_strings.LCPSERVER_PASSWORD
            configuration[
                "lcpserver_input_directory"
            ] = lcp_strings.LCPSERVER_INPUT_DIRECTORY
            configuration["provider_name"] = lcp_strings.PROVIDER_NAME
            configuration["passphrase_hint"] = lcp_strings.TEXT_HINT
            configuration[
                "encryption_algorithm"
            ] = LCPServerConstants.DEFAULT_ENCRYPTION_ALGORITHM

            # Act
            loan = lcp_api.checkout(patron, "pin", license_pool, "internal format")

            # Assert
            assert loan.collection_id == lcp_api_fixture.lcp_collection.id
            assert (
                loan.collection(lcp_api_fixture.db.session)
                == lcp_api_fixture.lcp_collection
            )
            assert loan.license_pool(lcp_api_fixture.db.session) == license_pool
            assert loan.data_source_name == data_source_name
            assert loan.identifier_type == license_pool.identifier.type
            assert loan.external_identifier == lcp_license["id"]
            assert loan.start_date == start_date
            assert loan.end_date == end_date

            lcp_server_mock.generate_license.assert_called_once_with(
                lcp_api_fixture.db.session,
                lcp_strings.CONTENT_ID,
                patron,
                start_date,
                end_date,
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_checkout_with_existing_loan(self, lcp_api_fixture):
        # Arrange
        lcp_api = LCPAPI(lcp_api_fixture.db.session, lcp_api_fixture.lcp_collection)
        patron = lcp_api_fixture.db.patron()
        days = lcp_api_fixture.lcp_collection.default_loan_period(patron.library)
        start_date = utc_now()
        end_date = start_date + datetime.timedelta(days=days)
        data_source = DataSource.lookup(
            lcp_api_fixture.db.session, DataSource.LCP, autocreate=True
        )
        data_source_name = data_source.name
        edition = lcp_api_fixture.db.edition(
            data_source_name=data_source_name, identifier_id=lcp_strings.CONTENT_ID
        )
        license_pool = lcp_api_fixture.db.licensepool(
            edition=edition,
            data_source_name=data_source_name,
            collection=lcp_api_fixture.lcp_collection,
        )
        lcp_license = json.loads(lcp_strings.LCPSERVER_LICENSE)
        lcp_server_mock = create_autospec(spec=LCPServer)
        lcp_server_mock.get_license = MagicMock(return_value=lcp_license)
        loan_identifier = "e99be177-4902-426a-9b96-0872ae877e2f"

        license_pool.loan_to(patron, external_identifier=loan_identifier)

        configuration = lcp_api_fixture.lcp_collection.integration_configuration
        with patch("api.lcp.collection.LCPServer") as lcp_server_constructor:
            lcp_server_constructor.return_value = lcp_server_mock

            configuration["lcpserver_url"] = lcp_strings.LCPSERVER_URL
            configuration["lcpserver_user"] = lcp_strings.LCPSERVER_USER
            configuration["lcpserver_password"] = lcp_strings.LCPSERVER_PASSWORD
            configuration[
                "lcpserver_input_directory"
            ] = lcp_strings.LCPSERVER_INPUT_DIRECTORY
            configuration["provider_name"] = lcp_strings.PROVIDER_NAME
            configuration["passphrase_hint"] = lcp_strings.TEXT_HINT
            configuration[
                "encryption_algorithm"
            ] = LCPServerConstants.DEFAULT_ENCRYPTION_ALGORITHM

            # Act
            loan = lcp_api.checkout(patron, "pin", license_pool, "internal format")

            # Assert
            assert loan.collection_id == lcp_api_fixture.lcp_collection.id
            assert (
                loan.collection(lcp_api_fixture.db.session)
                == lcp_api_fixture.lcp_collection
            )
            assert loan.license_pool(lcp_api_fixture.db.session) == license_pool
            assert loan.data_source_name == data_source_name
            assert loan.identifier_type == license_pool.identifier.type
            assert loan.external_identifier == loan_identifier
            assert loan.start_date == start_date
            assert loan.end_date == end_date

            lcp_server_mock.get_license.assert_called_once_with(
                lcp_api_fixture.db.session, loan_identifier, patron
            )

    @freeze_time("2020-01-01 00:00:00")
    def test_fulfil(self, lcp_api_fixture):
        # Arrange
        lcp_api = LCPAPI(lcp_api_fixture.db.session, lcp_api_fixture.lcp_collection)
        patron = lcp_api_fixture.db.patron()
        days = lcp_api_fixture.lcp_collection.default_loan_period(patron.library)
        today = utc_now()
        expires = today + datetime.timedelta(days=days)
        data_source = DataSource.lookup(
            lcp_api_fixture.db.session, DataSource.LCP, autocreate=True
        )
        data_source_name = data_source.name
        license_pool = lcp_api_fixture.db.licensepool(
            edition=None,
            data_source_name=data_source_name,
            collection=lcp_api_fixture.lcp_collection,
        )
        lcp_license = json.loads(lcp_strings.LCPSERVER_LICENSE)
        lcp_server_mock = create_autospec(spec=LCPServer)
        lcp_server_mock.get_license = MagicMock(return_value=lcp_license)

        configuration = lcp_api_fixture.lcp_collection.integration_configuration
        with patch("api.lcp.collection.LCPServer") as lcp_server_constructor:
            lcp_server_constructor.return_value = lcp_server_mock

            configuration["lcpserver_url"] = lcp_strings.LCPSERVER_URL
            configuration["lcpserver_user"] = lcp_strings.LCPSERVER_USER
            configuration["lcpserver_password"] = lcp_strings.LCPSERVER_PASSWORD
            configuration[
                "lcpserver_input_directory"
            ] = lcp_strings.LCPSERVER_INPUT_DIRECTORY

            configuration["provider_name"] = lcp_strings.PROVIDER_NAME
            configuration["passphrase_hint"] = lcp_strings.TEXT_HINT
            configuration[
                "encryption_algorithm"
            ] = LCPServerConstants.DEFAULT_ENCRYPTION_ALGORITHM

            # Act
            license_pool.loan_to(
                patron,
                start=today,
                end=expires,
                external_identifier=lcp_license["id"],
            )
            fulfilment_info = lcp_api.fulfill(
                patron, "pin", license_pool, "internal format"
            )

            # Assert
            assert isinstance(fulfilment_info, LCPFulfilmentInfo) == True
            assert fulfilment_info.collection_id == lcp_api_fixture.lcp_collection.id
            assert (
                fulfilment_info.collection(lcp_api_fixture.db.session)
                == lcp_api_fixture.lcp_collection
            )
            assert (
                fulfilment_info.license_pool(lcp_api_fixture.db.session) == license_pool
            )
            assert fulfilment_info.data_source_name == data_source_name
            assert fulfilment_info.identifier_type == license_pool.identifier.type

            lcp_server_mock.get_license.assert_called_once_with(
                lcp_api_fixture.db.session, lcp_license["id"], patron
            )

    def test_patron_activity_returns_correct_result(self, lcp_api_fixture):
        # Arrange
        lcp_api = LCPAPI(lcp_api_fixture.db.session, lcp_api_fixture.lcp_collection)

        # 1. Correct loan
        patron = lcp_api_fixture.db.patron()
        days = lcp_api_fixture.lcp_collection.default_loan_period(patron.library)
        today = utc_now()
        expires = today + datetime.timedelta(days=days)
        data_source = DataSource.lookup(
            lcp_api_fixture.db.session, DataSource.LCP, autocreate=True
        )
        data_source_name = data_source.name
        external_identifier = "1"
        license_pool = lcp_api_fixture.db.licensepool(
            edition=None,
            data_source_name=data_source_name,
            collection=lcp_api_fixture.lcp_collection,
        )
        license_pool.loan_to(
            patron, start=today, end=expires, external_identifier=external_identifier
        )

        # 2. Loan from a different collection
        other_collection = lcp_api_fixture.db.collection(
            protocol=ExternalIntegration.MANUAL
        )
        other_external_identifier = "2"
        other_license_pool = lcp_api_fixture.db.licensepool(
            edition=None, data_source_name=data_source_name, collection=other_collection
        )
        other_license_pool.loan_to(
            patron,
            start=today,
            end=expires,
            external_identifier=other_external_identifier,
        )

        # 3. Other patron's loan
        other_patron = lcp_api_fixture.db.patron()
        other_license_pool = lcp_api_fixture.db.licensepool(
            edition=None, data_source_name=data_source_name, collection=other_collection
        )
        other_license_pool.loan_to(other_patron, start=today, end=expires)

        # 4. Expired loan
        other_license_pool = lcp_api_fixture.db.licensepool(
            edition=None,
            data_source_name=data_source_name,
            collection=lcp_api_fixture.lcp_collection,
        )
        other_license_pool.loan_to(
            patron, start=today, end=today - datetime.timedelta(days=1)
        )

        # 5. Not started loan
        other_license_pool = lcp_api_fixture.db.licensepool(
            edition=None,
            data_source_name=data_source_name,
            collection=lcp_api_fixture.lcp_collection,
        )
        other_license_pool.loan_to(
            patron,
            start=today + datetime.timedelta(days=1),
            end=today + datetime.timedelta(days=2),
        )

        # Act
        loans = lcp_api.patron_activity(patron, "pin")

        # Assert
        assert len(loans) == 1

        loan = loans[0]
        assert loan.collection_id == lcp_api_fixture.lcp_collection.id
        assert (
            loan.collection(lcp_api_fixture.db.session)
            == lcp_api_fixture.lcp_collection
        )
        assert loan.license_pool(lcp_api_fixture.db.session) == license_pool
        assert loan.data_source_name == data_source_name
        assert loan.identifier_type == license_pool.identifier.type
        assert loan.external_identifier == external_identifier
        assert loan.start_date == today
        assert loan.end_date == expires
