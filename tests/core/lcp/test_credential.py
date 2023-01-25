from unittest.mock import patch

import pytest

from api.lcp.hash import HashingAlgorithm, UniversalHasher
from core.lcp.credential import (
    LCPCredentialFactory,
    LCPCredentialType,
    LCPHashedPassphrase,
    LCPUnhashedPassphrase,
)
from core.lcp.exceptions import LCPError
from core.model import Credential, DataSource, Patron
from tests.fixtures.database import DatabaseTransactionFixture


class TestLCPTypes:
    def test_bad_type_hashed(self):
        with pytest.raises(ValueError):
            LCPHashedPassphrase(23)

    def test_bad_type_unhashed(self):
        with pytest.raises(ValueError):
            LCPUnhashedPassphrase(23)

    def test_hashing(self):
        expected = LCPHashedPassphrase(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )
        received = LCPUnhashedPassphrase("hello").hash(
            UniversalHasher(HashingAlgorithm.SHA256.value)
        )
        assert expected == received


class ExampleCredentialFixture:
    factory: LCPCredentialFactory
    patron: Patron
    data_source: DataSource
    transaction: DatabaseTransactionFixture

    @classmethod
    def create(
        cls, transaction: DatabaseTransactionFixture
    ) -> "ExampleCredentialFixture":
        data = ExampleCredentialFixture()
        data.transaction = transaction
        data.factory = LCPCredentialFactory()
        data.patron = transaction.patron()
        data.data_source = DataSource.lookup(
            transaction.session, DataSource.INTERNAL_PROCESSING, autocreate=True
        )
        return data


@pytest.fixture()
def example_credential_fixture(
    db: DatabaseTransactionFixture,
) -> ExampleCredentialFixture:
    return ExampleCredentialFixture.create(db)


class TestCredentialFactory:
    def test_getter(self, example_credential_fixture: ExampleCredentialFixture):
        data = example_credential_fixture
        session = data.transaction.session

        credential_type = LCPCredentialType.PATRON_ID.value
        method_name = "get_patron_id"
        expected_result = "52a190d1-cd69-4794-9d7a-1ec50392697f"

        # Arrange
        credential = Credential(credential=expected_result)

        with patch.object(
            Credential, "persistent_token_create"
        ) as persistent_token_create_mock:
            persistent_token_create_mock.return_value = (credential, True)

            method = getattr(data.factory, method_name)

            # Act
            result = method(session, data.patron)

            # Assert
            assert result == expected_result
            persistent_token_create_mock.assert_called_once_with(
                session, data.data_source, credential_type, data.patron, None
            )

    def test_get_patron_passphrase(
        self, example_credential_fixture: ExampleCredentialFixture
    ):
        data = example_credential_fixture
        session = data.transaction.session

        # Arrange
        expected_result = LCPUnhashedPassphrase("12345")
        credential = Credential(credential=expected_result.text)

        with patch.object(
            Credential, "persistent_token_create"
        ) as persistent_token_create_mock:
            persistent_token_create_mock.return_value = (credential, True)

            result = data.factory.get_patron_passphrase(session, data.patron)

            # Assert
            assert result == expected_result
            persistent_token_create_mock.assert_called_once_with(
                session,
                data.data_source,
                LCPCredentialType.LCP_PASSPHRASE.value,
                data.patron,
                None,
            )

    def test_get_hashed_passphrase_raises_exception_when_there_is_no_passphrase(
        self, example_credential_fixture: ExampleCredentialFixture
    ):
        data = example_credential_fixture
        session = data.transaction.session

        # Act, assert
        with pytest.raises(LCPError):
            data.factory.get_hashed_passphrase(session, data.patron)

    def test_get_hashed_passphrase_returns_existing_hashed_passphrase(
        self, example_credential_fixture: ExampleCredentialFixture
    ):
        data = example_credential_fixture
        session = data.transaction.session

        # Arrange
        expected_result = LCPHashedPassphrase("12345")

        # Act
        data.factory.set_hashed_passphrase(session, data.patron, expected_result)
        result = data.factory.get_hashed_passphrase(session, data.patron)

        # Assert
        assert result == expected_result
