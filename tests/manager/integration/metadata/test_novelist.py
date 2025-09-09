from __future__ import annotations

import datetime
import json
from collections.abc import Generator
from functools import partial
from unittest.mock import MagicMock, create_autospec

import dateutil.parser
import pytest
from pytest import MonkeyPatch

from palace.manager.api.config import CannotLoadConfiguration
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.goals import Goals
from palace.manager.integration.metadata.novelist import (
    NoveListAPI,
    NoveListApiSettings,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.resource import HttpResponseTuple, Representation
from palace.manager.util.http.http import HTTP
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.mocks.mock import MockRequestsResponse


class NoveListFilesFixture(FilesFixture):
    """A fixture providing access to NoveList files."""

    def __init__(self) -> None:
        super().__init__("novelist")


@pytest.fixture()
def novelist_files_fixture() -> NoveListFilesFixture:
    """A fixture providing access to NoveList files."""
    return NoveListFilesFixture()


class NoveListFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, files: NoveListFilesFixture
    ) -> None:
        self.db = db
        self.files = files
        self.settings = NoveListApiSettings(username="library", password="yep")
        self.integration = db.integration_configuration(
            NoveListAPI,
            Goals.METADATA_GOAL,
            libraries=[db.default_library()],
            settings=self.settings,
        )
        self.novelist = NoveListAPI.from_config(db.default_library())

    def sample_data(self, filename: str) -> bytes:
        return self.files.sample_data(filename)

    def sample_representation(self, filename: str) -> Representation:
        content = self.sample_data(filename)
        return self.db.representation(media_type="application/json", content=content)[0]


@pytest.fixture(scope="function")
def novelist_fixture(
    db: DatabaseTransactionFixture, novelist_files_fixture: NoveListFilesFixture
) -> Generator[NoveListFixture]:
    fixture = NoveListFixture(db, novelist_files_fixture)
    yield fixture


class TestNoveListAPI:
    """Tests the NoveList API service object"""

    def test_from_config(self, novelist_fixture: NoveListFixture):
        """Confirms that NoveListAPI can be built from config successfully"""
        novelist = NoveListAPI.from_config(novelist_fixture.db.default_library())
        assert isinstance(novelist, NoveListAPI) is True
        assert novelist.profile == "library"
        assert novelist.password == "yep"

        # If the integration is not configured, an error is raised.
        another_library = novelist_fixture.db.library()
        pytest.raises(
            CannotLoadConfiguration,
            NoveListAPI.from_config,
            another_library,
        )

    def test_is_configured(self, novelist_fixture: NoveListFixture):
        # If a IntegrationLibraryConfiguration exists, the API is_configured
        assert (
            NoveListAPI.is_configured_db_check(novelist_fixture.db.default_library())
            is True
        )

        # If an integration doesn't exist for the library, it is not.
        library = novelist_fixture.db.library()
        assert NoveListAPI.is_configured_db_check(library) is False

    def test_review_response(self, novelist_fixture: NoveListFixture):
        invalid_credential_response: HttpResponseTuple = (
            403,
            {},
            b"HTML Access Denied page",
        )
        pytest.raises(
            Exception,
            novelist_fixture.novelist.review_response,
            invalid_credential_response,
        )

        missing_argument_response: HttpResponseTuple = (
            200,
            {},
            b'"Missing ISBN, UPC, or Client Identifier!"',
        )
        pytest.raises(
            Exception,
            novelist_fixture.novelist.review_response,
            missing_argument_response,
        )

        response: HttpResponseTuple = (200, {}, b"Here's the goods!")
        novelist_fixture.novelist.review_response(response)

    def test__extract_isbns(self) -> None:
        book_identifier = IdentifierData(type=Identifier.ISBN, identifier="12345")
        other_identifier = IdentifierData(type=Identifier.ISBN, identifier="67890")

        novelist_data = {
            "manifestations": [
                {"ISBN": book_identifier.identifier},
                {"ISBN": other_identifier.identifier},
            ]
        }

        # Without a filter all the ISBNs are returned
        result = NoveListAPI._extract_isbns(novelist_data)
        assert result == [book_identifier, other_identifier]

        # With a filter, only the ISBNs that don't match the filter are returned
        result = NoveListAPI._extract_isbns(novelist_data, filter={book_identifier})
        assert result == [other_identifier]

    def test__lookup_info_representation_to_recommendations(
        self, novelist_fixture: NoveListFixture
    ):
        bad_character = novelist_fixture.sample_representation("a_bad_character.json")
        novelist_id, recommendations = (
            novelist_fixture.novelist._lookup_info_representation_to_recommendations(
                bad_character
            )
        )

        assert novelist_id.type == Identifier.NOVELIST_ID
        assert novelist_id.identifier == "10392078"
        assert len(recommendations) == 625

        vampire = novelist_fixture.sample_representation("vampire_kisses.json")
        novelist_id, recommendations = (
            novelist_fixture.novelist._lookup_info_representation_to_recommendations(
                vampire
            )
        )
        assert novelist_id.type == Identifier.NOVELIST_ID
        assert novelist_id.identifier == "267510"
        assert len(recommendations) == 5

    def test_lookup_recommendations(
        self, novelist_fixture: NoveListFixture, http_client: MockHttpClientFixture
    ):
        # Test the lookup_recommendations() method.
        http_client.queue_response(200, media_type="text/html", content="yay")

        novelist = novelist_fixture.novelist

        mock_build_query_url = create_autospec(
            novelist.build_query_url, return_value="http://query-url/"
        )
        novelist.build_query_url = mock_build_query_url

        mock_scrubbed_url = create_autospec(
            novelist.scrubbed_url, return_value="http://scrubbed-url/"
        )
        novelist.scrubbed_url = mock_scrubbed_url

        mock_review_response = create_autospec(novelist.review_response)
        novelist.review_response = mock_review_response

        mock_lookup_info_representation_to_recommendations = create_autospec(
            novelist._lookup_info_representation_to_recommendations,
            return_value=(None, ["foo", "bar"]),
        )
        novelist._lookup_info_representation_to_recommendations = (
            mock_lookup_info_representation_to_recommendations
        )

        identifier = novelist_fixture.db.identifier(identifier_type=Identifier.ISBN)

        # Do the lookup.
        result = novelist.lookup_recommendations(identifier)

        # A number of parameters were passed into build_query_url() to
        # get the URL of the HTTP request. The same parameters were
        # also passed into scrubbed_url(), to get the URL that should
        # be used when storing the Representation in the database.
        assert mock_build_query_url.call_args == mock_scrubbed_url.call_args

        assert mock_build_query_url.call_args.args[0] == dict(
            profile=novelist.profile,
            ClientIdentifier=identifier.urn,
            ISBN=identifier.identifier,
            password=novelist.password,
            version=novelist.version,
        )

        # The HTTP request went out to the query URL -- not the scrubbed URL.
        assert ["http://query-url/"] == http_client.requests

        # The HTTP response was passed into novelist.review_response()
        mock_review_response.assert_called_once_with(
            (
                200,
                {"content-type": "text/html"},
                b"yay",
            )
        )

        # Finally, the Representation was passed into
        # lookup_info_to_metadata, which returned our mocked result
        assert ["foo", "bar"] == result

        # Looking at the Representation we can see that it was stored
        # in the database under its scrubbed URL, not the URL used to
        # make the request.
        mock_lookup_info_representation_to_recommendations.assert_called_once()
        rep = mock_lookup_info_representation_to_recommendations.call_args.args[0]
        assert "http://scrubbed-url/" == rep.url
        assert b"yay" == rep.content

    def test__lookup_info_representation_to_recommendations_ignores_empty_responses(
        self, novelist_fixture: NoveListFixture
    ):
        """API requests that return no data result return a None tuple"""

        null_response = novelist_fixture.sample_representation("null_data.json")
        result = (
            novelist_fixture.novelist._lookup_info_representation_to_recommendations(
                null_response
            )
        )
        assert result == (None, [])

        # This also happens when NoveList indicates with an empty
        # response that it doesn't know the ISBN.
        empty_response = novelist_fixture.sample_representation("unknown_isbn.json")
        result = (
            novelist_fixture.novelist._lookup_info_representation_to_recommendations(
                empty_response
            )
        )
        assert result == (None, [])

    def test_build_query_url(self, novelist_fixture: NoveListFixture):
        params = dict(
            ClientIdentifier="C I",
            ISBN="456",
            version="2.2",
            profile="username",
            password="secret",
        )

        # Authentication information is included in the URL by default
        full_result = novelist_fixture.novelist.build_query_url(params)
        auth_details = "&profile=username&password=secret"
        assert full_result.endswith(auth_details) is True
        assert "profile=username" in full_result
        assert "password=secret" in full_result

        # With a scrub, no authentication information is included.
        scrubbed_result = novelist_fixture.novelist.build_query_url(
            params, include_auth=False
        )
        assert scrubbed_result.endswith(auth_details) is False
        assert "profile=username" not in scrubbed_result
        assert "password=secret" not in scrubbed_result

        # Other details are urlencoded and available in both versions.
        for url in (scrubbed_result, full_result):
            assert "ClientIdentifier=C%20I" in url
            assert "ISBN=456" in url
            assert "version=2.2" in url

        # The method to create a scrubbed url returns the same result
        # as the NoveListAPI.build_query_url
        assert novelist_fixture.novelist.scrubbed_url(params) == scrubbed_result

    def test_confirm_same_identifier(self, novelist_fixture: NoveListFixture) -> None:
        identifier = IdentifierData(type=Identifier.NOVELIST_ID, identifier="84752928")
        unmatched_identifier = IdentifierData(
            type=Identifier.NOVELIST_ID, identifier="23781947"
        )

        assert (
            novelist_fixture.novelist._confirm_same_identifier(
                [(identifier, []), (unmatched_identifier, [])]
            )
            is False
        )
        assert (
            novelist_fixture.novelist._confirm_same_identifier(
                [(identifier, []), (identifier, [])]
            )
            is True
        )

    def test__lookup_recommendations_equivalent_isbns(
        self, novelist_fixture: NoveListFixture, db: DatabaseTransactionFixture
    ):
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        api = novelist_fixture.novelist
        mock_lookup = create_autospec(api._lookup_recommendations_isbn)
        api._lookup_recommendations_isbn = mock_lookup

        lookup_recommendations = partial(api.lookup_recommendations, identifier)

        # If there are no ISBN equivalents, we get an empty list
        assert lookup_recommendations() == []

        source = DataSource.lookup(db.session, DataSource.OVERDRIVE)
        identifier.equivalent_to(source, db.identifier(), strength=1)
        db.session.commit()
        assert lookup_recommendations() == []

        # If there's an ISBN equivalent, but it doesn't result in any data, we get an empty list
        isbn = db.identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn, strength=1)
        db.session.commit()
        mock_lookup.return_value = (None, [])
        assert lookup_recommendations() == []

        # Create an API class that can mockout NoveListAPI._choose_best_recommendations,
        # and make sure lookup returns something
        mock_choose_best = create_autospec(api._choose_best_recommendations)
        api._choose_best_recommendations = mock_choose_best

        # Give the identifier another ISBN equivalent.
        isbn2 = db.identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn2, strength=1)
        db.session.commit()

        # If choose_best_metadata returns 0 confidence metadata, the
        # lookup returns an empty list.
        mock_lookup.reset_mock()
        mock_novelist_id = IdentifierData(type=Identifier.NOVELIST_ID, identifier="123")
        mock_recommendations = [
            IdentifierData(type=Identifier.ISBN, identifier="456"),
            IdentifierData(type=Identifier.ISBN, identifier="789"),
        ]
        mock_lookup.return_value = (mock_novelist_id, mock_recommendations)
        mock_choose_best.return_value = ([], 0.0)
        assert lookup_recommendations() == []

        # Lookup was performed for both ISBNs.
        assert mock_lookup.call_count == 2

        # If choose_best_metadata returns a low confidence metadata, the
        # lookup returns None.
        mock_choose_best.return_value = (mock_recommendations, 0.33)
        assert lookup_recommendations() == []

        # If choose_best_metadata returns a high confidence metadata, the
        # lookup returns the metadata.
        mock_choose_best.return_value = (mock_recommendations, 0.67)
        assert lookup_recommendations() is mock_recommendations

    def test__choose_best_recommendations(self, novelist_fixture: NoveListFixture):
        more_identifier = IdentifierData(type=Identifier.NOVELIST_ID, identifier="more")
        less_identifier = IdentifierData(type=Identifier.NOVELIST_ID, identifier="less")
        mock_recommendations = MagicMock()
        other_recommendations = MagicMock()

        # When only one object is given, that object is returned.
        result, confidence = novelist_fixture.novelist._choose_best_recommendations(
            [(more_identifier, mock_recommendations)], MagicMock()
        )
        assert result is mock_recommendations
        # A default confidence of 1.0 is returned.
        assert confidence == 1.0

        # When top identifiers have equal representation, the method returns 0 confidence.
        result, confidence = novelist_fixture.novelist._choose_best_recommendations(
            [
                (more_identifier, mock_recommendations),
                (less_identifier, other_recommendations),
            ],
            MagicMock(),
        )
        assert result == []
        assert confidence == 0.0

        # But when one pulls ahead, we get the metadata object again.
        result, confidence = novelist_fixture.novelist._choose_best_recommendations(
            [
                (more_identifier, mock_recommendations),
                (more_identifier, mock_recommendations),
                (less_identifier, other_recommendations),
            ],
            MagicMock(),
        )
        assert result == mock_recommendations
        assert round(confidence, 2) == 0.67

    def test_get_items_from_query(self, novelist_fixture: NoveListFixture):
        items = novelist_fixture.novelist.get_items_from_query(
            novelist_fixture.db.default_library()
        )
        # There are no books in the current library.
        assert [] == items

        # Set up a book for this library.
        edition = novelist_fixture.db.edition(
            identifier_type=Identifier.ISBN,
            publication_date=dateutil.parser.parse("2012-01-01"),
        )
        pool = novelist_fixture.db.licensepool(
            edition, collection=novelist_fixture.db.default_collection()
        )
        contributor = novelist_fixture.db.contributor(
            sort_name=edition.sort_author, name=edition.author
        )

        items = novelist_fixture.novelist.get_items_from_query(
            novelist_fixture.db.default_library()
        )

        item = dict(
            author=contributor[0]._sort_name,
            title=edition.title,
            mediaType=novelist_fixture.novelist.medium_to_book_format_type_values.get(
                edition.medium, ""
            ),
            isbn=edition.primary_identifier.identifier,
            distributor=edition.data_source.name,
            publicationDate=edition.published.strftime("%Y%m%d"),
        )

        assert [item] == items

    def test_create_item_object(self, novelist_fixture: NoveListFixture):
        # We pass no identifier or item to process so we get nothing back.
        (
            currentIdentifier,
            existingItem,
            newItem,
            addItem,
        ) = novelist_fixture.novelist.create_item_object(None, None, None)
        assert currentIdentifier is None
        assert existingItem is None
        assert newItem is None
        assert addItem is False

        # Item row from the db query
        # (identifier, identifier type, identifier,
        # edition title, edition medium, edition published date,
        # contribution role, contributor sort name
        # distributor)
        book1_from_query = (
            "12345",
            "Axis 360 ID",
            "23456",
            "Title 1",
            "Book",
            datetime.date(2002, 1, 1),
            "Author",
            "Author 1",
            "Gutenberg",
        )
        book1_from_query_primary_author = (
            "12345",
            "Axis 360 ID",
            "23456",
            "Title 1",
            "Book",
            datetime.date(2002, 1, 1),
            "Primary Author",
            "Author 2",
            "Gutenberg",
        )
        book1_narrator_from_query = (
            "12345",
            "Axis 360 ID",
            "23456",
            "Title 1",
            "Book",
            datetime.date(2002, 1, 1),
            "Narrator",
            "Narrator 1",
            "Gutenberg",
        )
        book2_from_query = (
            "34567",
            "Axis 360 ID",
            "56789",
            "Title 2",
            "Book",
            datetime.date(1414, 1, 1),
            "Author",
            "Author 3",
            "Gutenberg",
        )

        (currentIdentifier, existingItem, newItem, addItem) = (
            # params: new item, identifier, existing item
            novelist_fixture.novelist.create_item_object(book1_from_query, None, None)
        )
        assert currentIdentifier == book1_from_query[2]
        assert existingItem is None
        assert newItem == {
            "isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "role": "Author",
            "author": "Author 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101",
        }
        # We want to still process this item along with the next one in case
        # the following one has the same ISBN.
        assert addItem is False

        # Note that `newItem` is what we get from the previous call from `create_item_object`.
        # We are now processing the previous object along with the new one.
        # This is to check and update the value for `author` if the role changes
        # to `Primary Author`.
        (
            currentIdentifier,
            existingItem,
            newItem,
            addItem,
        ) = novelist_fixture.novelist.create_item_object(
            book1_from_query_primary_author, currentIdentifier, newItem
        )
        assert currentIdentifier == book1_from_query[2]
        assert existingItem == {
            "isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "author": "Author 2",
            "role": "Primary Author",
            "distributor": "Gutenberg",
            "publicationDate": "20020101",
        }
        assert newItem is None
        assert addItem is False

        # Test that a narrator gets added along with an author.
        (
            currentIdentifier,
            existingItem,
            newItem,
            addItem,
        ) = novelist_fixture.novelist.create_item_object(
            book1_narrator_from_query, currentIdentifier, existingItem
        )
        assert currentIdentifier == book1_narrator_from_query[2]
        assert existingItem == {
            "isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "author": "Author 2",
            # The role has been updated to author since the last processed item
            # has an author role. This property is eventually removed before
            # sending to Novelist so it's not really important.
            "role": "Narrator",
            "narrator": "Narrator 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101",
        }
        assert newItem is None
        assert addItem is False

        # New Object
        (
            currentIdentifier,
            existingItem,
            newItem,
            addItem,
        ) = novelist_fixture.novelist.create_item_object(
            book2_from_query, currentIdentifier, existingItem
        )
        assert currentIdentifier == book2_from_query[2]
        assert existingItem == {
            "isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "author": "Author 2",
            "role": "Narrator",
            "narrator": "Narrator 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101",
        }
        assert newItem == {
            "isbn": "56789",
            "mediaType": "EBook",
            "title": "Title 2",
            "role": "Author",
            "author": "Author 3",
            "distributor": "Gutenberg",
            "publicationDate": "14140101",
        }
        assert addItem is True

        # New Object
        # Test that a narrator got added but not an author
        (
            currentIdentifier,
            existingItem,
            newItem,
            addItem,
        ) = novelist_fixture.novelist.create_item_object(
            book1_narrator_from_query, None, None
        )

        assert currentIdentifier == book1_narrator_from_query[2]
        assert existingItem is None
        assert newItem == {
            "isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "role": "Narrator",
            "narrator": "Narrator 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101",
        }
        assert addItem is False

    def test_put_items_novelist(self, novelist_fixture: NoveListFixture):
        mock_http_put = create_autospec(novelist_fixture.novelist.put)
        novelist_fixture.novelist.put = mock_http_put
        mock_http_put.side_effect = Exception("Failed to put items")

        # No items, so put never gets called and none gets returned
        response = novelist_fixture.novelist.put_items_novelist(
            novelist_fixture.db.default_library()
        )
        assert response is None
        assert mock_http_put.call_count == 0

        edition = novelist_fixture.db.edition(identifier_type=Identifier.ISBN)
        pool = novelist_fixture.db.licensepool(
            edition, collection=novelist_fixture.db.default_collection()
        )
        mock_response = {"Customer": "NYPL", "RecordsReceived": 10}
        mock_http_put.side_effect = None
        mock_http_put.return_value = MockRequestsResponse(
            200, content=json.dumps(mock_response)
        )

        response = novelist_fixture.novelist.put_items_novelist(
            novelist_fixture.db.default_library()
        )

        assert response == mock_response

    def test_make_novelist_data_object(self, novelist_fixture: NoveListFixture):
        bad_data: list[dict[str, str]] = []
        result = novelist_fixture.novelist.make_novelist_data_object(bad_data)

        assert result == {"customer": "library:yep", "records": []}

        data = [
            {
                "isbn": "12345",
                "mediaType": "http://schema.org/EBook",
                "title": "Book 1",
                "author": "Author 1",
            },
            {
                "isbn": "12346",
                "mediaType": "http://schema.org/EBook",
                "title": "Book 2",
                "author": "Author 2",
            },
        ]
        result = novelist_fixture.novelist.make_novelist_data_object(data)

        assert result == {"customer": "library:yep", "records": data}

    def test_put(self, novelist_fixture: NoveListFixture, monkeypatch: MonkeyPatch):
        mock_put = create_autospec(HTTP.put_with_timeout)
        monkeypatch.setattr(HTTP, "put_with_timeout", mock_put)

        headers = {"AuthorizedIdentifier": "authorized!"}
        data = ["12345", "12346", "12347"]

        novelist_fixture.novelist.put("http://apiendpoint.com", headers, data=data)
        mock_put.assert_called_once_with(
            "http://apiendpoint.com", data=data, headers=headers, timeout=None
        )
