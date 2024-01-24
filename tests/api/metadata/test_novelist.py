import datetime
import json
from unittest.mock import MagicMock, create_autospec

import pytest
from _pytest.monkeypatch import MonkeyPatch

from api.config import CannotLoadConfiguration
from api.metadata.novelist import NoveListAPI, NoveListApiSettings
from core.integration.goals import Goals
from core.metadata_layer import Metadata
from core.model import DataSource, Identifier
from core.util.http import HTTP
from tests.core.mock import DummyHTTPClient, MockRequestsResponse
from tests.fixtures.api_novelist_files import NoveListFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture


class NoveListFixture:
    def __init__(self, db: DatabaseTransactionFixture, files: NoveListFilesFixture):
        self.db = db
        self.files = files
        self.settings = NoveListApiSettings(username="library", password="yep")
        self.integration = db.integration_configuration(
            "NoveList Select",
            Goals.METADATA_GOAL,
            libraries=[db.default_library()],
        )
        NoveListAPI.settings_update(self.integration, self.settings)
        self.novelist = NoveListAPI.from_config(db.default_library())

    def sample_data(self, filename):
        return self.files.sample_data(filename)

    def sample_representation(self, filename):
        content = self.sample_data(filename)
        return self.db.representation(media_type="application/json", content=content)[0]


@pytest.fixture(scope="function")
def novelist_fixture(
    db: DatabaseTransactionFixture, api_novelist_files_fixture: NoveListFilesFixture
):
    fixture = NoveListFixture(db, api_novelist_files_fixture)
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
        assert NoveListAPI.is_configured(novelist_fixture.db.default_library()) is True

        # If an ExternalIntegration doesn't exist for the library, it is not.
        library = novelist_fixture.db.library()
        assert NoveListAPI.is_configured(library) is False

    def test_review_response(self, novelist_fixture: NoveListFixture):
        invalid_credential_response: tuple[int, dict[str, str], bytes] = (
            403,
            {},
            b"HTML Access Denied page",
        )
        pytest.raises(
            Exception,
            novelist_fixture.novelist.review_response,
            invalid_credential_response,
        )

        missing_argument_response: tuple[int, dict[str, str], bytes] = (
            200,
            {},
            b'"Missing ISBN, UPC, or Client Identifier!"',
        )
        pytest.raises(
            Exception,
            novelist_fixture.novelist.review_response,
            missing_argument_response,
        )

        response: tuple[int, dict[str, str], bytes] = (200, {}, b"Here's the goods!")
        novelist_fixture.novelist.review_response(response)

    def test_lookup_info_to_metadata(self, novelist_fixture: NoveListFixture):
        # Basic book information is returned
        identifier, ignore = Identifier.for_foreign_id(
            novelist_fixture.db.session, Identifier.ISBN, "9780804171335"
        )
        bad_character = novelist_fixture.sample_representation("a_bad_character.json")
        metadata = novelist_fixture.novelist.lookup_info_to_metadata(bad_character)

        assert isinstance(metadata, Metadata)
        assert metadata.primary_identifier.type == Identifier.NOVELIST_ID
        assert metadata.primary_identifier.identifier == "10392078"
        assert metadata.title == "A bad character"
        assert metadata.subtitle is None
        assert len(metadata.contributors) == 1
        [contributor] = metadata.contributors
        assert contributor.sort_name == "Kapoor, Deepti"
        assert len(metadata.identifiers) == 4
        assert len(metadata.subjects) == 4
        assert len(metadata.measurements) == 2
        ratings = sorted(metadata.measurements, key=lambda m: m.value)
        assert ratings[0].value == 2
        assert ratings[1].value == 3.27
        assert len(metadata.recommendations) == 625

        # Confirm that Lexile and series data is extracted with a
        # different sample.
        vampire = novelist_fixture.sample_representation("vampire_kisses.json")
        metadata = novelist_fixture.novelist.lookup_info_to_metadata(vampire)
        assert isinstance(metadata, Metadata)
        [lexile] = [s for s in metadata.subjects if s.type == "Lexile"]
        assert lexile.identifier == "630"
        assert metadata.series == "Vampire kisses manga"
        # The full title should be selected, since every volume
        # has the same main title: 'Vampire kisses'
        assert metadata.title == "Vampire kisses: blood relatives. Volume 1"
        assert metadata.series_position == 1
        assert len(metadata.recommendations) == 5

    def test_get_series_information(self, novelist_fixture: NoveListFixture):
        metadata = Metadata(data_source=DataSource.NOVELIST)
        vampire = json.loads(novelist_fixture.sample_data("vampire_kisses.json"))
        book_info = vampire["TitleInfo"]
        series_info = vampire["FeatureContent"]["SeriesInfo"]

        (metadata, ideal_title_key) = novelist_fixture.novelist.get_series_information(
            metadata, series_info, book_info
        )
        # Relevant series information is extracted
        assert metadata.series == "Vampire kisses manga"
        assert metadata.series_position == 1
        # The 'full_title' key should be returned as ideal because
        # all the volumes have the same 'main_title'
        assert ideal_title_key == "full_title"

        watchman = json.loads(
            novelist_fixture.sample_data("alternate_series_example.json")
        )
        book_info = watchman["TitleInfo"]
        series_info = watchman["FeatureContent"]["SeriesInfo"]
        # Confirms that the new example doesn't match any volume's full title
        assert [] == [
            v
            for v in series_info["series_titles"]
            if v.get("full_title") == book_info.get("full_title")
        ]

        # But it still finds its matching volume
        (metadata, ideal_title_key) = novelist_fixture.novelist.get_series_information(
            metadata, series_info, book_info
        )
        assert metadata.series == "Elvis Cole/Joe Pike novels"
        assert metadata.series_position == 11
        # And recommends using the main_title
        assert ideal_title_key == "main_title"

        # If the volume is found in the series more than once...
        book_info = dict(
            main_title="The Baby-Sitters Club",
            full_title="The Baby-Sitters Club: Claudia and Mean Janine",
        )
        series_info = dict(
            full_title="The Baby-Sitters Club series",
            series_titles=[
                # The volume is here twice!
                book_info,
                book_info,
                dict(
                    full_title="The Baby-Sitters Club",
                    main_title="The Baby-Sitters Club: Claudia and Mean Janine",
                    series_position="3.",
                ),
            ],
        )
        # An error is raised.
        pytest.raises(
            ValueError,
            novelist_fixture.novelist.get_series_information,
            metadata,
            series_info,
            book_info,
        )

    def test_lookup(self, novelist_fixture: NoveListFixture):
        # Test the lookup() method.
        h = DummyHTTPClient()
        h.queue_response(200, "text/html", content="yay")

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

        mock_lookup_info_to_metadata = create_autospec(
            novelist.lookup_info_to_metadata, return_value="some metadata"
        )
        novelist.lookup_info_to_metadata = mock_lookup_info_to_metadata

        identifier = novelist_fixture.db.identifier(identifier_type=Identifier.ISBN)

        # Do the lookup.
        result = novelist.lookup(identifier, do_get=h.do_get)

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
        assert ["http://query-url/"] == h.requests

        # The HTTP response was passed into novelist.review_response()
        mock_review_response.assert_called_once_with(
            (
                200,
                {"content-type": "text/html"},
                b"yay",
            )
        )

        # Finally, the Representation was passed into
        # lookup_info_to_metadata, which returned a hard-coded string
        # as the final result.
        assert "some metadata" == result

        # Looking at the Representation we can see that it was stored
        # in the database under its scrubbed URL, not the URL used to
        # make the request.
        mock_lookup_info_to_metadata.assert_called_once()
        rep = mock_lookup_info_to_metadata.call_args.args[0]
        assert "http://scrubbed-url/" == rep.url
        assert b"yay" == rep.content

    def test_lookup_info_to_metadata_ignores_empty_responses(
        self, novelist_fixture: NoveListFixture
    ):
        """API requests that return no data result return a None tuple"""

        null_response = novelist_fixture.sample_representation("null_data.json")
        result = novelist_fixture.novelist.lookup_info_to_metadata(null_response)
        assert result is None

        # This also happens when NoveList indicates with an empty
        # response that it doesn't know the ISBN.
        empty_response = novelist_fixture.sample_representation("unknown_isbn.json")
        result = novelist_fixture.novelist.lookup_info_to_metadata(empty_response)
        assert result is None

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

    def test_scrub_subtitle(self, novelist_fixture: NoveListFixture):
        """Unnecessary title segments are removed from subtitles"""

        scrub = novelist_fixture.novelist._scrub_subtitle
        assert scrub(None) is None
        assert scrub("[electronic resource]") is None
        assert scrub("[electronic resource] :  ") is None
        assert scrub("[electronic resource] :  A Biomythography") == "A Biomythography"

    def test_confirm_same_identifier(self, novelist_fixture: NoveListFixture):
        source = DataSource.lookup(novelist_fixture.db.session, DataSource.NOVELIST)
        identifier, ignore = Identifier.for_foreign_id(
            novelist_fixture.db.session, Identifier.NOVELIST_ID, "84752928"
        )
        unmatched_identifier, ignore = Identifier.for_foreign_id(
            novelist_fixture.db.session, Identifier.NOVELIST_ID, "23781947"
        )
        metadata = Metadata(source, primary_identifier=identifier)
        match = Metadata(source, primary_identifier=identifier)
        mistake = Metadata(source, primary_identifier=unmatched_identifier)

        assert (
            novelist_fixture.novelist._confirm_same_identifier([metadata, mistake])
            is False
        )
        assert (
            novelist_fixture.novelist._confirm_same_identifier([metadata, match])
            is True
        )

    def test_lookup_equivalent_isbns(self, novelist_fixture: NoveListFixture):
        identifier = novelist_fixture.db.identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        api = novelist_fixture.novelist
        mock_lookup = create_autospec(api.lookup)
        api.lookup = mock_lookup

        # If there are no ISBN equivalents, it returns None.
        assert api.lookup_equivalent_isbns(identifier) is None

        source = DataSource.lookup(novelist_fixture.db.session, DataSource.OVERDRIVE)
        identifier.equivalent_to(source, novelist_fixture.db.identifier(), strength=1)
        novelist_fixture.db.session.commit()
        assert api.lookup_equivalent_isbns(identifier) is None

        # If there's an ISBN equivalent, but it doesn't result in metadata,
        # it returns none.
        isbn = novelist_fixture.db.identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn, strength=1)
        novelist_fixture.db.session.commit()
        mock_lookup.return_value = None
        assert api.lookup_equivalent_isbns(identifier) is None

        # Create an API class that can mockout NoveListAPI.choose_best_metadata,
        # and make sure lookup returns something
        mock_choose_best_metadata = create_autospec(api.choose_best_metadata)
        api.choose_best_metadata = mock_choose_best_metadata
        mock_lookup.return_value = create_autospec(Metadata)

        # Give the identifier another ISBN equivalent.
        isbn2 = novelist_fixture.db.identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn2, strength=1)
        novelist_fixture.db.session.commit()

        # If choose_best_metadata returns None, the lookup returns None.
        mock_lookup.reset_mock()
        mock_choose_best_metadata.return_value = (None, None)
        assert api.lookup_equivalent_isbns(identifier) is None

        # Lookup was performed for both ISBNs.
        assert mock_lookup.call_count == 2

        # If choose_best_metadata returns a low confidence metadata, the
        # lookup returns None.
        mock_best_metadata = MagicMock()
        mock_choose_best_metadata.return_value = (mock_best_metadata, 0.33)
        assert api.lookup_equivalent_isbns(identifier) is None

        # If choose_best_metadata returns a high confidence metadata, the
        # lookup returns the metadata.
        mock_choose_best_metadata.return_value = (mock_best_metadata, 0.67)
        assert api.lookup_equivalent_isbns(identifier) is mock_best_metadata

    def test_choose_best_metadata(self, novelist_fixture: NoveListFixture):
        more_identifier = novelist_fixture.db.identifier(
            identifier_type=Identifier.NOVELIST_ID
        )
        less_identifier = novelist_fixture.db.identifier(
            identifier_type=Identifier.NOVELIST_ID
        )
        metadatas = [Metadata(DataSource.NOVELIST, primary_identifier=more_identifier)]

        # When only one Metadata object is given, that object is returned.
        result = novelist_fixture.novelist.choose_best_metadata(
            metadatas, novelist_fixture.db.identifier()
        )
        assert isinstance(result, tuple) is True
        assert result[0] == metadatas[0]
        # A default confidence of 1.0 is returned.
        assert result[1] == 1.0

        # When top identifiers have equal representation, the method returns none.
        metadatas.append(
            Metadata(DataSource.NOVELIST, primary_identifier=less_identifier)
        )
        assert novelist_fixture.novelist.choose_best_metadata(
            metadatas, novelist_fixture.db.identifier()
        ) == (None, None)

        # But when one pulls ahead, we get the metadata object again.
        metadatas.append(
            Metadata(DataSource.NOVELIST, primary_identifier=more_identifier)
        )
        result = novelist_fixture.novelist.choose_best_metadata(
            metadatas, novelist_fixture.db.identifier()
        )
        assert isinstance(result, tuple)
        metadata, confidence = result
        assert isinstance(metadata, Metadata)
        assert isinstance(confidence, float)
        assert round(confidence, 2) == 0.67
        assert metadata.primary_identifier == more_identifier

    def test_get_items_from_query(self, novelist_fixture: NoveListFixture):
        items = novelist_fixture.novelist.get_items_from_query(
            novelist_fixture.db.default_library()
        )
        # There are no books in the current library.
        assert [] == items

        # Set up a book for this library.
        edition = novelist_fixture.db.edition(
            identifier_type=Identifier.ISBN, publication_date="2012-01-01"
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
            "http://apiendpoint.com", data, headers=headers, timeout=None
        )
