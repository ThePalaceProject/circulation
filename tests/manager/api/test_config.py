from collections import Counter

from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA

from palace.manager.api.config import Configuration
from palace.manager.integration.configuration.library import LibrarySettings
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


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
            LibrarySettings.model_construct(large_collection_languages=["spa", "jpn"])  # type: ignore[call-arg]
        )
        assert C.large_collection_languages(library) == ["jpn", "spa"]

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
        assert 0 == max_fines

        # A more lenient approach.
        settings.max_outstanding_fines = 100.0
        max_fines = m(library)
        assert max_fines is not None
        assert 100 == max_fines
