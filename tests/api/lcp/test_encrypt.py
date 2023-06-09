from unittest.mock import patch

import pytest
from pyfakefs.fake_filesystem_unittest import Patcher

from api.lcp.collection import LCPAPI
from api.lcp.encrypt import (
    LCPEncryptionConstants,
    LCPEncryptionException,
    LCPEncryptionResult,
    LCPEncryptor,
)
from core.integration.goals import Goals
from core.model import Identifier
from core.model.integration import IntegrationConfiguration
from tests.api.lcp import lcp_strings
from tests.fixtures.database import DatabaseTransactionFixture


class LCPEncryptFixture:
    db: DatabaseTransactionFixture
    integration: IntegrationConfiguration

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.integration = self.db.integration_configuration(
            protocol=LCPAPI.NAME, goal=Goals.LICENSE_GOAL
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
        lcp_encrypt_fixture: LCPEncryptFixture,
        _,
        file_path,
        lcpencrypt_output,
        expected_result,
        expected_exception,
        create_file,
    ):
        # Arrange
        # integration_owner = create_autospec(spec=HasIntegrationConfiguration)
        # integration_owner.integration_configuration = MagicMock(
        #     return_value=lcp_encrypt_fixture.integration
        # )
        configuration = lcp_encrypt_fixture.integration
        encryptor = LCPEncryptor(configuration)
        identifier = Identifier(identifier=lcp_strings.BOOK_IDENTIFIER)

        configuration[
            "lcpencrypt_location"
        ] = LCPEncryptionConstants.DEFAULT_LCPENCRYPT_LOCATION

        with Patcher() as patcher:
            patcher.fs.create_file(LCPEncryptionConstants.DEFAULT_LCPENCRYPT_LOCATION)

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
