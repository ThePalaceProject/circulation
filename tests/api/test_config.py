import json
import os
from collections import Counter
from contextlib import nullcontext as does_not_raise
from unittest.mock import patch

import pytest
from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA

from api.config import Configuration
from core.config import CannotLoadConfiguration
from core.configuration.library import LibrarySettings
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture
from tests.fixtures.library import LibraryFixture


@pytest.fixture()
def notifications_files_fixture() -> FilesFixture:
    """Provides access to notifications test files."""
    return FilesFixture("util/notifications")


class TestConfiguration:
    def test_cipher(self, db: DatabaseTransactionFixture):
        # Test the cipher() helper method.

        # Generate a public/private key pair.
        key = RSA.generate(2048)
        cipher = PKCS1_OAEP.new(key)
        public = key.publickey().exportKey()
        private = key.exportKey()

        # Pass the public key into cipher() to get something that can
        # encrypt.
        encryptor = Configuration.cipher(public)
        encrypted = encryptor.encrypt(b"some text")

        # Pass the private key into cipher() to get something that can
        # decrypt.
        decryptor = Configuration.cipher(private)
        decrypted = decryptor.decrypt(encrypted)
        assert b"some text" == decrypted

    def test_collection_language_method_performs_estimate(
        self, db: DatabaseTransactionFixture
    ):
        C = Configuration
        library = db.default_library()

        # We haven't set any of these values.
        assert library.settings.large_collection_languages is None
        assert library.settings.small_collection_languages is None
        assert library.settings.tiny_collection_languages is None

        # So how does this happen?
        assert C.large_collection_languages(library) == ["eng"]
        assert C.small_collection_languages(library) == []
        assert C.tiny_collection_languages(library) == []

        # It happens because the first time we call one of those
        # *_collection_languages, it estimates values for all three
        # configuration settings, based on the library's current
        # holdings.
        assert library.settings.large_collection_languages == ["eng"]
        assert library.settings.small_collection_languages == []
        assert library.settings.tiny_collection_languages == []

        # We can change these values.
        library.update_settings(
            LibrarySettings.construct(large_collection_languages=["spa", "jpn"])
        )
        assert C.large_collection_languages(library) == ["spa", "jpn"]

    def test_estimate_language_collection_for_library(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # We thought we'd have big collections.
        settings = library_fixture.mock_settings()
        settings.large_collection_languages = ["spa", "fre"]
        settings.small_collection_languages = ["chi"]
        settings.tiny_collection_languages = ["rus"]
        library = library_fixture.library(settings=settings)

        # But there's nothing in our database, so when we call
        # Configuration.estimate_language_collections_for_library...
        Configuration.estimate_language_collections_for_library(library)

        # ...it gets reset to the default.
        assert library.settings.large_collection_languages == ["eng"]
        assert library.settings.small_collection_languages == []
        assert library.settings.tiny_collection_languages == []

    def test_classify_holdings(self, db: DatabaseTransactionFixture):
        m = Configuration.classify_holdings

        # If there are no titles in the collection at all, we assume
        # there will eventually be a large English collection.
        assert [["eng"], [], []] == m(Counter())

        # The largest collection is given the 'large collection' treatment,
        # even if it's very small.
        very_small = Counter(rus=2, pol=1)
        assert [["rus"], [], ["pol"]] == m(very_small)

        # Otherwise, the classification of a collection depends on the
        # sheer number of items in that collection. Within a
        # classification, languages are ordered by holding size.
        different_sizes = Counter(
            jpn=16000, fre=20000, spa=8000, nav=6, ukr=4000, ira=1500
        )
        assert [["fre", "jpn"], ["spa", "ukr", "ira"], ["nav"]] == m(different_sizes)

    def test_max_outstanding_fines(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        m = Configuration.max_outstanding_fines

        library = library_fixture.library()
        settings = library_fixture.settings(library)

        # By default, fines are not enforced.
        assert m(library) is None

        # Any amount of fines is too much.
        settings.max_outstanding_fines = 0
        max_fines = m(library)
        assert max_fines is not None
        assert 0 == max_fines.amount

        # A more lenient approach.
        settings.max_outstanding_fines = 100.0
        max_fines = m(library)
        assert max_fines is not None
        assert 100 == max_fines.amount

    @patch.object(os, "environ", new=dict())
    def test_fcm_credentials(self, notifications_files_fixture):
        invalid_json = "{ this is invalid JSON }"
        valid_credentials_json = notifications_files_fixture.sample_text(
            "fcm-credentials-valid-json.json"
        )
        valid_credentials_object = json.loads(valid_credentials_json)

        # No FCM credentials environment variable present.
        with pytest.raises(
            CannotLoadConfiguration,
            match=r"FCM Credentials configuration environment variable not defined.",
        ):
            Configuration.fcm_credentials()

        # Non-existent file.
        os.environ[
            Configuration.FCM_CREDENTIALS_FILE_ENVIRONMENT_VARIABLE
        ] = "filedoesnotexist.deleteifitdoes"
        with pytest.raises(
            FileNotFoundError,
            match=r"The FCM credentials file .* does not exist.",
        ):
            Configuration.fcm_credentials()

        # Valid JSON file.
        os.environ[
            Configuration.FCM_CREDENTIALS_FILE_ENVIRONMENT_VARIABLE
        ] = notifications_files_fixture.sample_path("fcm-credentials-valid-json.json")
        assert valid_credentials_object == Configuration.fcm_credentials()

        # Setting more than one FCM credentials environment variable is not valid.
        os.environ[
            Configuration.FCM_CREDENTIALS_JSON_ENVIRONMENT_VARIABLE
        ] = valid_credentials_json
        with pytest.raises(
            CannotLoadConfiguration,
            match=r"Both JSON .* and file-based .* FCM Credential environment variables are defined, but only one is allowed.",
        ):
            Configuration.fcm_credentials()

        # Down to just the JSON FCM credentials environment variable.
        del os.environ[Configuration.FCM_CREDENTIALS_FILE_ENVIRONMENT_VARIABLE]
        assert valid_credentials_object == Configuration.fcm_credentials()

        # But we should get an exception if the JSON is invalid.
        os.environ[
            Configuration.FCM_CREDENTIALS_JSON_ENVIRONMENT_VARIABLE
        ] = invalid_json
        with pytest.raises(
            CannotLoadConfiguration,
            match=r"Cannot parse value of FCM credential environment variable .* as JSON.",
        ):
            Configuration.fcm_credentials()

    @pytest.mark.parametrize(
        "env_var_value, expected_result, raises_exception",
        [
            ["true", True, False],
            ["True", True, False],
            [None, False, False],
            ["", False, False],
            ["false", False, False],
            ["False", False, False],
            ["3", None, True],
            ["X", None, True],
        ],
    )
    @patch.object(os, "environ", new=dict())
    def test_basic_token_auth_is_enabled(
        self, env_var_value, expected_result, raises_exception
    ):
        env_var = Configuration.BASIC_TOKEN_AUTH_ENABLED_ENVVAR

        # Simulate an unset environment variable with the `None` value.
        if env_var_value is None:
            del os.environ[env_var]
        else:
            os.environ[env_var] = env_var_value

        expected_exception = (
            pytest.raises(
                CannotLoadConfiguration,
                match=f"Invalid value for {env_var} environment variable.",
            )
            if raises_exception
            else does_not_raise()
        )

        with expected_exception:
            assert expected_result == Configuration.basic_token_auth_is_enabled()
