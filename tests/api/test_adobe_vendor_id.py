from __future__ import annotations

import base64
import datetime
from typing import Type
from unittest.mock import MagicMock

import pytest
from jwt import DecodeError, ExpiredSignatureError, InvalidIssuedAtError
from sqlalchemy import select

from api.adobe_vendor_id import AuthdataUtility
from core.config import CannotLoadConfiguration
from core.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStatus,
)
from core.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.vendor_id import VendorIDFixture


@pytest.fixture(scope="function")
def authdata() -> AuthdataUtility:
    return AuthdataUtility(
        vendor_id="The Vendor ID",
        library_uri="http://my-library.org/",
        library_short_name="MyLibrary",
        secret="My library secret",
    )


class TestAuthdataUtility:
    @pytest.mark.parametrize(
        "registration_status, authdata_utility_type",
        [
            (RegistrationStatus.SUCCESS, AuthdataUtility),
            (RegistrationStatus.FAILURE, type(None)),
        ],
    )
    def test_eligible_authdata_vendor_id_integrations(
        self,
        registration_status: RegistrationStatus,
        authdata_utility_type: Type[AuthdataUtility] | Type[None],
        authdata: AuthdataUtility,
        vendor_id_fixture: VendorIDFixture,
    ):
        # Only a discovery integration with a successful registration for
        # a given library is eligible to provide an AuthdataUtility.
        library = vendor_id_fixture.db.default_library()
        vendor_id_fixture.initialize_adobe(library)
        vendor_id_fixture.registration.status = registration_status
        utility = AuthdataUtility.from_config(library)

        assert isinstance(utility, authdata_utility_type)

    def test_from_config(
        self,
        authdata: AuthdataUtility,
        vendor_id_fixture: VendorIDFixture,
        library_fixture: LibraryFixture,
    ):
        library = library_fixture.library()
        vendor_id_fixture.initialize_adobe(library)
        library_url = library.settings.website

        utility = AuthdataUtility.from_config(library)
        assert utility is not None
        assert library.short_name is not None

        registration = vendor_id_fixture.db.session.scalars(
            select(DiscoveryServiceRegistration).where(
                DiscoveryServiceRegistration.library_id == library.id,
                DiscoveryServiceRegistration.integration_id
                == vendor_id_fixture.registry.id,
            )
        ).first()
        assert registration is not None
        assert registration.short_name == library.short_name + "token"
        assert registration.shared_secret == library.short_name + " token secret"

        assert utility.vendor_id == VendorIDFixture.TEST_VENDOR_ID
        assert utility.library_uri == library_url

        # If the Library object is disconnected from its database
        # session, as may happen in production...
        vendor_id_fixture.db.session.expunge(library)

        # Then an attempt to use it to get an AuthdataUtility
        # will fail...
        with pytest.raises(ValueError) as excinfo:
            AuthdataUtility.from_config(library)
        assert (
            "No database connection provided and could not derive one from Library object!"
            in str(excinfo.value)
        )

        # ...unless a database session is provided in the constructor.
        authdata_2 = AuthdataUtility.from_config(library, vendor_id_fixture.db.session)
        assert isinstance(authdata_2, AuthdataUtility)
        library = vendor_id_fixture.db.session.merge(library)
        vendor_id_fixture.db.session.commit()

        # If an integration is set up but incomplete, from_config
        # raises CannotLoadConfiguration.
        old_short_name = registration.short_name
        registration.short_name = None
        pytest.raises(CannotLoadConfiguration, AuthdataUtility.from_config, library)
        registration.short_name = old_short_name

        library_settings = library_fixture.settings(library)
        old_website = library_settings.website
        library_settings.website = None  # type: ignore[assignment]
        pytest.raises(CannotLoadConfiguration, AuthdataUtility.from_config, library)
        library_settings.website = old_website

        old_secret = registration.shared_secret
        registration.shared_secret = None
        pytest.raises(CannotLoadConfiguration, AuthdataUtility.from_config, library)
        registration.shared_secret = old_secret

        # If there is no Adobe Vendor ID integration set up,
        # from_config() returns None.
        vendor_id_fixture.db.session.delete(registration)
        assert AuthdataUtility.from_config(library) is None

    def test_short_client_token_for_patron(
        self, authdata: AuthdataUtility, db: DatabaseTransactionFixture
    ):
        class MockAuthdataUtility(AuthdataUtility):
            def __init__(self):
                pass

            def encode_short_client_token(self, patron_identifier):
                self.encode_sct_called_with = patron_identifier
                return "a", "b"

            def _adobe_patron_identifier(self, patron_information):
                self.patron_identifier_called_with = patron_information
                return "patron identifier"

        # A patron is passed in; we get their identifier for Adobe ID purposes,
        # and generate a short client token based on it
        patron = db.patron()
        authdata = MockAuthdataUtility()
        sct = authdata.short_client_token_for_patron(patron)
        assert patron == authdata.patron_identifier_called_with
        assert authdata.encode_sct_called_with == "patron identifier"
        assert sct == ("a", "b")
        # The identifier for Adobe ID purposes is passed in, and we use it directly.
        authdata.short_client_token_for_patron("identifier for Adobe ID purposes")
        assert sct == ("a", "b")
        assert authdata.encode_sct_called_with == "identifier for Adobe ID purposes"

    def test_decode_round_trip(self, authdata: AuthdataUtility):
        patron_identifier = "Patron identifier"
        vendor_id, authdata_bytes = authdata.encode(patron_identifier)
        assert "The Vendor ID" == vendor_id

        # We can decode the authdata with our secret.
        decoded = authdata.decode(authdata_bytes)
        assert ("http://my-library.org/", "Patron identifier") == decoded

    def test_decode_round_trip_with_intermediate_mischief(
        self, authdata: AuthdataUtility
    ):
        patron_identifier = "Patron identifier"
        vendor_id, authdata_bytes = authdata.encode(patron_identifier)
        assert "The Vendor ID" == vendor_id

        # A mischievious party in the middle decodes our authdata
        # without telling us.
        authdata_other_bytes = base64.decodebytes(authdata_bytes)

        # But it still works.
        decoded = authdata.decode(authdata_other_bytes)
        assert ("http://my-library.org/", "Patron identifier") == decoded

    def test_encode(self, authdata: AuthdataUtility):
        # Test that _encode gives a known value with known input.
        patron_identifier = "Patron identifier"
        now = datetime_utc(2016, 1, 1, 12, 0, 0)
        expires = datetime_utc(2018, 1, 1, 12, 0, 0)
        authdata_encoded = authdata._encode(
            authdata.library_uri, patron_identifier, now, expires
        )
        assert (
            base64.encodebytes(
                b"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vbXktbGlicmFyeS5vcmcvIiwic3ViIjoiUGF0cm9uIGlkZW50aWZpZXIiLCJpYXQiOjE0NTE2NDk2MDAuMCwiZXhwIjoxNTE0ODA4MDAwLjB9.wKAnFfJVfJP55CIyD7PntFZrtWVTwDcXHjL-quTndzc"
            )
            == authdata_encoded
        )

    def test_decode_from_unknown_library_fails(self, authdata: AuthdataUtility):
        # Here's the AuthdataUtility used by a library we don't know
        # about.
        foreign_authdata = AuthdataUtility(
            vendor_id="The Vendor ID",
            library_uri="http://some-other-library.org/",
            library_short_name="SomeOther",
            secret="Some other library secret",
        )
        vendor_id, authdata_bytes = foreign_authdata.encode("A patron")
        # They can encode, but we cna't decode.
        with pytest.raises(DecodeError) as excinfo:
            authdata.decode(authdata_bytes)
        assert "Unknown library: http://some-other-library.org/" in str(excinfo.value)

    def test_cannot_decode_token_from_future(self, authdata: AuthdataUtility):
        future = utc_now() + datetime.timedelta(days=365)
        authdata_bytes = authdata._encode("Patron identifier", iat=future)
        pytest.raises(InvalidIssuedAtError, authdata.decode, authdata_bytes)

    def test_cannot_decode_expired_token(self, authdata: AuthdataUtility):
        expires = datetime_utc(2016, 1, 1, 12, 0, 0)
        authdata_bytes = authdata._encode("Patron identifier", exp=expires)
        pytest.raises(ExpiredSignatureError, authdata.decode, authdata_bytes)

    def test_cannot_encode_null_patron_identifier(self, authdata: AuthdataUtility):
        with pytest.raises(ValueError) as excinfo:
            authdata.encode(None)
        assert "No patron identifier specified" in str(excinfo.value)

    def test_cannot_decode_null_patron_identifier(self, authdata: AuthdataUtility):
        authdata_bytes = authdata._encode(
            authdata.library_uri,
            None,
        )
        with pytest.raises(DecodeError) as excinfo:
            authdata.decode(authdata_bytes)
        assert "No subject specified" in str(excinfo.value)

    def test_short_client_token_round_trip(self, authdata: AuthdataUtility):
        # Encoding a token and immediately decoding it gives the expected
        # result.
        vendor_id, token = authdata.encode_short_client_token("a patron")
        assert authdata.vendor_id == vendor_id

        library_uri, patron = authdata.decode_short_client_token(token)
        assert authdata.library_uri == library_uri
        assert "a patron" == patron

    def test_short_client_token_encode_known_value(self, authdata: AuthdataUtility):
        # Verify that the encoding algorithm gives a known value on known
        # input.
        value = authdata._encode_short_client_token(
            "a library", "a patron identifier", 1234.5
        )

        # Note the colon characters that replaced the plus signs in
        # what would otherwise be normal base64 text. Similarly, for
        # the semicolon which replaced the slash, and the at sign which
        # replaced the equals sign.
        assert (
            "a library|1234.5|a patron identifier|YoNGn7f38mF531KSWJ;o1H0Z3chbC:uTE:t7pAwqYxM@"
            == value
        )

        # Dissect the known value to show how it works.
        token, signature = value.rsplit("|", 1)

        # Signature is base64-encoded in a custom way that avoids
        # triggering an Adobe bug ; token is not.
        signature_bytes = AuthdataUtility.adobe_base64_decode(signature)

        # The token comes from the library name, the patron identifier,
        # and the time of creation.
        assert "a library|1234.5|a patron identifier" == token

        # The signature comes from signing the token with the
        # secret associated with this library.
        expect_signature = authdata.short_token_signer.sign(
            token.encode("utf-8"), authdata.short_token_signing_key
        )
        assert expect_signature == signature_bytes

    def test_encode_short_client_token_expiry(self, monkeypatch):
        authdata = AuthdataUtility(
            vendor_id="The Vendor ID",
            library_uri="http://your-library.org/",
            library_short_name="you",
            secret="Your library secret",
        )
        test_date = datetime_utc(2021, 5, 5)
        monkeypatch.setattr(authdata, "_now", lambda: test_date)
        assert authdata._now() == test_date

        patron_identifier = "Patron identifier"

        # Test with no expiry set
        vendor_id, token = authdata.encode_short_client_token(patron_identifier)
        assert token.split("|")[0:-1] == ["YOU", "1620176400", "Patron identifier"]

        # Test with expiry set to 20 min
        vendor_id, token = authdata.encode_short_client_token(
            patron_identifier, {"minutes": 20}
        )
        assert token.split("|")[0:-1] == ["YOU", "1620174000", "Patron identifier"]

        # Test with expiry set to 2 days
        vendor_id, token = authdata.encode_short_client_token(
            patron_identifier, {"days": 2}
        )
        assert token.split("|")[0:-1] == ["YOU", "1620345600", "Patron identifier"]

        # Test with expiry set to 4 hours
        vendor_id, token = authdata.encode_short_client_token(
            patron_identifier, {"hours": 4}
        )
        assert token.split("|")[0:-1] == ["YOU", "1620187200", "Patron identifier"]

    def test_decode_client_token_errors(self, authdata: AuthdataUtility):
        # Test various token errors
        m = authdata._decode_short_client_token

        # A token has to contain at least two pipe characters.
        with pytest.raises(ValueError) as excinfo:
            m("foo|", b"signature")
        assert "Invalid client token" in str(excinfo.value)

        # The expiration time must be numeric.
        with pytest.raises(ValueError) as excinfo:
            m("library|a time|patron", b"signature")
        assert 'Expiration time "a time" is not numeric' in str(excinfo.value)

        # The patron identifier must not be blank.
        with pytest.raises(ValueError) as excinfo:
            m("library|1234|", b"signature")
        assert "Token library|1234| has empty patron identifier" in str(excinfo.value)

        # The library must be a known one.
        with pytest.raises(ValueError) as excinfo:
            m("library|1234|patron", b"signature")
        assert 'I don\'t know how to handle tokens from library "LIBRARY"' in str(
            excinfo.value
        )

        # The token must not have expired.
        with pytest.raises(ValueError) as excinfo:
            m("mylibrary|1234|patron", b"signature")
        assert "Token mylibrary|1234|patron expired at 1970-01-01 00:20:34" in str(
            excinfo.value
        )

        # Finally, the signature must be valid.
        with pytest.raises(ValueError) as excinfo:
            m("mylibrary|99999999999|patron", b"signature")
        assert "Invalid signature for" in str(excinfo.value)

    def test_adobe_base64_encode_decode(self):
        # Test our special variant of base64 encoding designed to avoid
        # triggering an Adobe bug.
        value = "!\tFN6~'Es52?X!#)Z*_S"

        encoded = AuthdataUtility.adobe_base64_encode(value)
        assert "IQlGTjZ:J0VzNTI;WCEjKVoqX1M@" == encoded

        # This is like normal base64 encoding, but with a colon
        # replacing the plus character, a semicolon replacing the
        # slash, an at sign replacing the equal sign and the final
        # newline stripped.
        assert encoded.replace(":", "+").replace(";", "/").replace(
            "@", "="
        ) + "\n" == base64.encodebytes(value.encode("utf-8")).decode("utf-8")

        # We can reverse the encoding to get the original value.
        assert value == AuthdataUtility.adobe_base64_decode(encoded).decode("utf-8")

    def test__encode_short_client_token_uses_adobe_base64_encoding(
        self, authdata: AuthdataUtility
    ):
        MockSigner = MagicMock()
        # Always return the same signature, crafted to contain a
        # plus sign, a slash and an equal sign when base64-encoded.
        MockSigner.sign.return_value = "!\tFN6~'Es52?X!#)Z*_S"
        authdata.short_token_signer = MockSigner
        token = authdata._encode_short_client_token("lib", "1234", 0)

        # The signature part of the token has been encoded with our
        # custom encoding, not vanilla base64.
        assert "lib|0|1234|IQlGTjZ:J0VzNTI;WCEjKVoqX1M@" == token

    def test_decode_two_part_short_client_token_uses_adobe_base64_encoding(self):
        # The base64 encoding of this signature has a plus sign in it.
        signature = "LbU}66%\\-4zt>R>_)\n2Q"
        encoded_signature = AuthdataUtility.adobe_base64_encode(signature)

        # We replace the plus sign with a colon.
        assert ":" in encoded_signature
        assert "+" not in encoded_signature

        # Make sure that decode_two_part_short_client_token properly
        # reverses that change when decoding the 'password'.
        class MockAuthdataUtility(AuthdataUtility):
            def _decode_short_client_token(self, token, supposed_signature):
                assert supposed_signature.decode("utf-8") == signature
                self.test_code_ran = True

        utility = MockAuthdataUtility(
            vendor_id="The Vendor ID",
            library_uri="http://your-library.org/",
            library_short_name="you",
            secret="Your library secret",
        )
        utility.test_code_ran = False
        utility.decode_two_part_short_client_token("username", encoded_signature)

        # The code in _decode_short_client_token ran. Since there was no
        # test failure, it ran successfully.
        assert utility.test_code_ran is True
