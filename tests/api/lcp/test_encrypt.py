from unittest.mock import MagicMock, create_autospec, patch

import pytest
from pyfakefs.fake_filesystem_unittest import Patcher

from api.lcp.collection import LCPAPI
from api.lcp.encrypt import (
    LCPEncryptionConfiguration,
    LCPEncryptionException,
    LCPEncryptionResult,
    LCPEncryptor,
)
from core.model import Identifier
from core.model.configuration import (
    ConfigurationFactory,
    ConfigurationStorage,
    ExternalIntegration,
    HasExternalIntegration,
)
from tests.api.lcp import lcp_strings
from tests.fixtures.database import DatabaseTransactionFixture


class LCPEncryptFixture:
    db: DatabaseTransactionFixture
    integration: ExternalIntegration

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.integration = self.db.external_integration(
            protocol=LCPAPI.NAME, goal=ExternalIntegration.LICENSE_GOAL
        )


@pytest.fixture(scope="function")
def lcp_encrypt_fixture(db: DatabaseTransactionFixture) -> LCPEncryptFixture:
    return LCPEncryptFixture(db)


class TestLCPEncryptor:
    @pytest.mark.parametrize(
        "_, file_path, lcpencrypt_output, expected_result, expected_exception, create_file",
        [
            (
                "non_existing_directory",
                lcp_strings.NOT_EXISTING_BOOK_FILE_PATH,
                lcp_strings.LCPENCRYPT_NOT_EXISTING_DIRECTORY_RESULT,
                None,
                LCPEncryptionException(
                    lcp_strings.LCPENCRYPT_NOT_EXISTING_DIRECTORY_RESULT.strip()
                ),
                False,
            ),
            (
                "failed_encryption",
                lcp_strings.NOT_EXISTING_BOOK_FILE_PATH,
                lcp_strings.LCPENCRYPT_FAILED_ENCRYPTION_RESULT,
                None,
                LCPEncryptionException("Encryption failed"),
                True,
            ),
            (
                "successful_encryption",
                lcp_strings.EXISTING_BOOK_FILE_PATH,
                lcp_strings.LCPENCRYPT_SUCCESSFUL_ENCRYPTION_RESULT,
                LCPEncryptionResult(
                    content_id=lcp_strings.BOOK_IDENTIFIER,
                    content_encryption_key=lcp_strings.CONTENT_ENCRYPTION_KEY,
                    protected_content_location=lcp_strings.PROTECTED_CONTENT_LOCATION,
                    protected_content_disposition=lcp_strings.PROTECTED_CONTENT_DISPOSITION,
                    protected_content_type=lcp_strings.PROTECTED_CONTENT_TYPE,
                    protected_content_length=lcp_strings.PROTECTED_CONTENT_LENGTH,
                    protected_content_sha256=lcp_strings.PROTECTED_CONTENT_SHA256,
                ),
                None,
                True,
            ),
            (
                "failed_lcp_server_notification",
                lcp_strings.EXISTING_BOOK_FILE_PATH,
                lcp_strings.LCPENCRYPT_FAILED_LCPSERVER_NOTIFICATION,
                None,
                LCPEncryptionException(
                    lcp_strings.LCPENCRYPT_FAILED_LCPSERVER_NOTIFICATION.strip()
                ),
                True,
            ),
            (
                "successful_lcp_server_notification",
                lcp_strings.EXISTING_BOOK_FILE_PATH,
                lcp_strings.LCPENCRYPT_SUCCESSFUL_NOTIFICATION_RESULT,
                LCPEncryptionResult(
                    content_id=lcp_strings.BOOK_IDENTIFIER,
                    content_encryption_key=lcp_strings.CONTENT_ENCRYPTION_KEY,
                    protected_content_location=lcp_strings.PROTECTED_CONTENT_LOCATION,
                    protected_content_disposition=lcp_strings.PROTECTED_CONTENT_DISPOSITION,
                    protected_content_type=lcp_strings.PROTECTED_CONTENT_TYPE,
                    protected_content_length=lcp_strings.PROTECTED_CONTENT_LENGTH,
                    protected_content_sha256=lcp_strings.PROTECTED_CONTENT_SHA256,
                ),
                None,
                True,
            ),
        ],
    )
    def test_local_lcpencrypt(
        self,
        lcp_encrypt_fixture,
        _,
        file_path,
        lcpencrypt_output,
        expected_result,
        expected_exception,
        create_file,
    ):
        # Arrange
        integration_owner = create_autospec(spec=HasExternalIntegration)
        integration_owner.external_integration = MagicMock(
            return_value=lcp_encrypt_fixture.integration
        )
        configuration_storage = ConfigurationStorage(integration_owner)
        configuration_factory = ConfigurationFactory()
        encryptor = LCPEncryptor(configuration_storage, configuration_factory)
        identifier = Identifier(identifier=lcp_strings.BOOK_IDENTIFIER)

        with configuration_factory.create(
            configuration_storage,
            lcp_encrypt_fixture.db.session,
            LCPEncryptionConfiguration,
        ) as configuration:
            configuration.lcpencrypt_location = (
                LCPEncryptionConfiguration.DEFAULT_LCPENCRYPT_LOCATION
            )

            with Patcher() as patcher:
                patcher.fs.create_file(
                    LCPEncryptionConfiguration.DEFAULT_LCPENCRYPT_LOCATION
                )

                if create_file:
                    patcher.fs.create_file(file_path)

                with patch("subprocess.check_output") as subprocess_check_output_mock:
                    subprocess_check_output_mock.return_value = lcpencrypt_output

                    if expected_exception:
                        with pytest.raises(
                            expected_exception.__class__
                        ) as exception_metadata:
                            encryptor.encrypt(
                                lcp_encrypt_fixture.db.session,
                                file_path,
                                identifier.identifier,
                            )

                        # Assert
                        assert exception_metadata.value == expected_exception
                    else:
                        # Assert
                        result = encryptor.encrypt(
                            lcp_encrypt_fixture.db.session,
                            file_path,
                            identifier.identifier,
                        )
                        assert result == expected_result
