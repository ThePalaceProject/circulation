from __future__ import annotations

import datetime
import json
import os
import urllib.parse
from typing import Literal
from unittest.mock import MagicMock

import pytest
import requests_mock

from api.lcp import utils
from api.lcp.encrypt import LCPEncryptionResult
from api.lcp.hash import HasherFactory
from api.lcp.server import LCPServer, LCPServerConfiguration, LCPServerSettings
from core.lcp.credential import LCPCredentialFactory, LCPUnhashedPassphrase
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from tests.api.lcp import lcp_strings
from tests.fixtures.database import DatabaseTransactionFixture


class LCPServerFixture:
    db: DatabaseTransactionFixture
    lcp_collection: Collection
    integration: ExternalIntegration
    hasher_factory: HasherFactory
    credential_factory: LCPCredentialFactory
    lcp_server: LCPServer

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.lcp_collection = self.db.collection(protocol=ExternalIntegration.LCP)
        self.configuration = self.lcp_collection.integration_configuration
        self.configuration["lcpserver_input_directory"] = "/tmp"
        self.hasher_factory = HasherFactory()
        self.credential_factory = LCPCredentialFactory()
        print(self.lcp_collection.integration_configuration.settings)
        self.lcp_server = LCPServer(
            lambda: LCPServerSettings(**self.configuration.settings),
            self.hasher_factory,
            self.credential_factory,
        )


@pytest.fixture(scope="function")
def lcp_server_fixture(db: DatabaseTransactionFixture) -> LCPServerFixture:
    return LCPServerFixture(db)


class TestLCPServer:
    @pytest.mark.parametrize(
        "_, input_directory",
        [
            ("non_empty_input_directory", "/tmp/encrypted_books"),
        ],
    )
    def test_add_content(
        self,
        lcp_server_fixture: LCPServerFixture,
        _: Literal["empty_input_directory", "non_empty_input_directory"],
        input_directory: Literal["", "/tmp/encrypted_books"],
    ):
        # Arrange
        lcp_server = LCPServer(
            lambda: LCPServerSettings(**lcp_server_fixture.configuration.settings),
            lcp_server_fixture.hasher_factory,
            lcp_server_fixture.credential_factory,
        )
        encrypted_content = LCPEncryptionResult(
            content_id=lcp_strings.CONTENT_ID,
            content_encryption_key="12345",
            protected_content_location="/opt/readium/files/encrypted",
            protected_content_disposition="encrypted_book",
            protected_content_type="application/epub+zip",
            protected_content_length=12345,
            protected_content_sha256="12345",
        )
        expected_protected_content_disposition = os.path.join(
            input_directory, encrypted_content.protected_content_disposition
        )

        configuration = lcp_server_fixture.configuration
        configuration["lcpserver_url"] = lcp_strings.LCPSERVER_URL
        configuration["lcpserver_user"] = lcp_strings.LCPSERVER_USER
        configuration["lcpserver_password"] = lcp_strings.LCPSERVER_PASSWORD
        configuration["lcpserver_input_directory"] = input_directory
        configuration["provider_name"] = lcp_strings.PROVIDER_NAME
        configuration["passphrase_hint"] = lcp_strings.TEXT_HINT
        configuration[
            "encryption_algorithm"
        ] = LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM

        with requests_mock.Mocker() as request_mock:
            url = urllib.parse.urljoin(
                lcp_strings.LCPSERVER_URL, f"/contents/{lcp_strings.CONTENT_ID}"
            )
            request_mock.put(url)

            # Act
            lcp_server.add_content(lcp_server_fixture.db.session, encrypted_content)

            # Assert
            assert request_mock.called == True

            json_request = json.loads(request_mock.last_request.text)
            assert json_request["content-id"] == encrypted_content.content_id
            assert (
                json_request["content-encryption-key"]
                == encrypted_content.content_encryption_key
            )
            assert (
                json_request["protected-content-location"]
                == expected_protected_content_disposition
            )
            assert (
                json_request["protected-content-disposition"]
                == encrypted_content.protected_content_disposition
            )
            assert (
                json_request["protected-content-type"]
                == encrypted_content.protected_content_type
            )
            assert (
                json_request["protected-content-length"]
                == encrypted_content.protected_content_length
            )
            assert (
                json_request["protected-content-sha256"]
                == encrypted_content.protected_content_sha256
            )

    @pytest.mark.parametrize(
        "_, license_start, license_end, max_printable_pages, max_copiable_pages",
        [
            ("none_rights", None, None, None, None),
            (
                "license_start",
                datetime.datetime(2020, 1, 1, 00, 00, 00),
                None,
                None,
                None,
            ),
            (
                "license_end",
                None,
                datetime.datetime(2020, 12, 31, 23, 59, 59),
                None,
                None,
            ),
            ("max_printable_pages", None, None, 10, None),
            ("max_printable_pages_empty_max_copiable_pages", None, None, 10, ""),
            ("empty_max_printable_pages", None, None, "", None),
            ("max_copiable_pages", None, None, None, 1024),
            ("empty_max_printable_pages_max_copiable_pages", None, None, "", 1024),
            ("empty_max_copiable_pages", None, None, None, ""),
            (
                "dates",
                datetime.datetime(2020, 1, 1, 00, 00, 00),
                datetime.datetime(2020, 12, 31, 23, 59, 59),
                None,
                None,
            ),
            (
                "full_rights",
                datetime.datetime(2020, 1, 1, 00, 00, 00),
                datetime.datetime(2020, 12, 31, 23, 59, 59),
                10,
                1024,
            ),
        ],
    )
    def test_generate_license(
        self,
        lcp_server_fixture: LCPServerFixture,
        _: Literal[
            "none_rights",
            "license_start",
            "license_end",
            "max_printable_pages",
            "max_printable_pages_empty_max_copiable_pages",
            "empty_max_printable_pages",
            "max_copiable_pages",
            "empty_max_printable_pages_max_copiable_pages",
            "empty_max_copiable_pages",
            "dates",
            "full_rights",
        ],
        license_start: datetime.datetime | None,
        license_end: datetime.datetime | None,
        max_printable_pages: Literal[10, ""] | None,
        max_copiable_pages: Literal["", 1024] | None,
    ):
        # Arrange
        patron = lcp_server_fixture.db.patron()
        expected_patron_id = "52a190d1-cd69-4794-9d7a-1ec50392697f"
        expected_patron_passphrase = LCPUnhashedPassphrase(
            "52a190d1-cd69-4794-9d7a-1ec50392697a"
        )
        expected_patron_key = lcp_server_fixture.hasher_factory.create(
            LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM
        ).hash(expected_patron_passphrase.text)

        configuration = lcp_server_fixture.configuration
        configuration["lcpserver_url"] = lcp_strings.LCPSERVER_URL
        configuration["lcpserver_user"] = lcp_strings.LCPSERVER_USER
        configuration["lcpserver_password"] = lcp_strings.LCPSERVER_PASSWORD
        configuration["provider_name"] = lcp_strings.PROVIDER_NAME
        configuration["passphrase_hint"] = lcp_strings.TEXT_HINT
        configuration[
            "encryption_algorithm"
        ] = LCPServerConfiguration.DEFAULT_ENCRYPTION_ALGORITHM
        configuration["max_printable_pages"] = max_printable_pages
        configuration["max_copiable_pages"] = max_copiable_pages

        lcp_server_fixture.credential_factory.get_patron_id = MagicMock(  # type: ignore
            return_value=expected_patron_id
        )
        lcp_server_fixture.credential_factory.get_patron_passphrase = MagicMock(  # type: ignore
            return_value=expected_patron_passphrase
        )

        with requests_mock.Mocker() as request_mock:
            url = urllib.parse.urljoin(
                lcp_strings.LCPSERVER_URL,
                f"/contents/{lcp_strings.CONTENT_ID}/license",
            )
            request_mock.post(url, json=lcp_strings.LCPSERVER_LICENSE)

            # Act
            license = lcp_server_fixture.lcp_server.generate_license(
                lcp_server_fixture.db.session,
                lcp_strings.CONTENT_ID,
                patron,
                license_start,
                license_end,
            )

            # Assert
            assert request_mock.called == True
            assert license == lcp_strings.LCPSERVER_LICENSE

            json_request = json.loads(request_mock.last_request.text)
            assert json_request["provider"] == lcp_strings.PROVIDER_NAME
            assert json_request["user"]["id"] == expected_patron_id
            assert (
                json_request["encryption"]["user_key"]["text_hint"]
                == lcp_strings.TEXT_HINT
            )
            assert (
                json_request["encryption"]["user_key"]["hex_value"]
                == expected_patron_key
            )

            if license_start is not None:
                assert json_request["rights"]["start"] == utils.format_datetime(
                    license_start
                )
            if license_end is not None:
                assert json_request["rights"]["end"] == utils.format_datetime(
                    license_end
                )
            if max_printable_pages is not None and max_printable_pages != "":
                assert json_request["rights"]["print"] == max_printable_pages
            if max_copiable_pages is not None and max_copiable_pages != "":
                assert json_request["rights"]["copy"] == max_copiable_pages

            all_rights_fields_are_empty = all(
                [
                    rights_field is None or rights_field == ""
                    for rights_field in [
                        license_start,
                        license_end,
                        max_printable_pages,
                        max_copiable_pages,
                    ]
                ]
            )
            if all_rights_fields_are_empty:
                assert ("rights" in json_request) == False

            lcp_server_fixture.credential_factory.get_patron_id.assert_called_once_with(
                lcp_server_fixture.db.session, patron
            )
            lcp_server_fixture.credential_factory.get_patron_passphrase.assert_called_once_with(
                lcp_server_fixture.db.session, patron
            )
