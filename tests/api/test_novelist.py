import datetime
import json

import pytest

from api.config import CannotLoadConfiguration
from api.novelist import MockNoveListAPI, NoveListAPI
from core.metadata_layer import Metadata
from core.model import DataSource, ExternalIntegration, Identifier
from core.util.http import HTTP
from tests.core.mock import DummyHTTPClient, MockRequestsResponse
from tests.fixtures.api_novelist_files import NoveListFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture


class NoveListFixture:
    db: DatabaseTransactionFixture
    files: NoveListFilesFixture
    integration: ExternalIntegration
    novelist: NoveListAPI

    def __init__(self, db: DatabaseTransactionFixture, files: NoveListFilesFixture):
        self.db = db
        self.files = files
        self.integration = db.external_integration(
            ExternalIntegration.NOVELIST,
            ExternalIntegration.METADATA_GOAL,
            username="library",
            password="yep",
            libraries=[db.default_library()],
        )
        self.novelist = NoveListAPI.from_config(db.default_library())

    def sample_data(self, filename):
        return self.files.sample_data(filename)

    def sample_representation(self, filename):
        content = self.sample_data(filename)
        return self.db.representation(media_type="application/json", content=content)[0]

    def close(self):
        NoveListAPI.IS_CONFIGURED = None


@pytest.fixture(scope="function")
def novelist_fixture(
    db: DatabaseTransactionFixture, api_novelist_files_fixture: NoveListFilesFixture
):
    fixture = NoveListFixture(db, api_novelist_files_fixture)
    yield fixture
    fixture.close()


class TestNoveListAPI:
    """Tests the NoveList API service object"""

    def test_from_config(self, novelist_fixture: NoveListFixture):
        """Confirms that NoveListAPI can be built from config successfully"""
        novelist = NoveListAPI.from_config(novelist_fixture.db.default_library())
        assert True == isinstance(novelist, NoveListAPI)
        assert "library" == novelist.profile
        assert "yep" == novelist.password

        # Without either configuration value, an error is raised.
        novelist_fixture.integration.password = None
        pytest.raises(
            CannotLoadConfiguration,
            NoveListAPI.from_config,
            novelist_fixture.db.default_library(),
        )

        novelist_fixture.integration.password = "yep"
        novelist_fixture.integration.username = None
        pytest.raises(
            CannotLoadConfiguration,
            NoveListAPI.from_config,
            novelist_fixture.db.default_library(),
        )

    def test_is_configured(self, novelist_fixture: NoveListFixture):
        # If an ExternalIntegration exists, the API is_configured
        assert True == NoveListAPI.is_configured(novelist_fixture.db.default_library())
        # A class variable is set to reduce future database requests.
        assert (
            novelist_fixture.db.default_library().id
            == NoveListAPI._configuration_library_id
        )

        # If an ExternalIntegration doesn't exist for the library, it is not.
        library = novelist_fixture.db.library()
        assert False == NoveListAPI.is_configured(library)
        # And the class variable is updated.
        assert library.id == NoveListAPI._configuration_library_id

    def test_review_response(self, novelist_fixture: NoveListFixture):
        invalid_credential_response = (403, {}, b"HTML Access Denied page")  # type: ignore
        pytest.raises(
            Exception,
            novelist_fixture.novelist.review_response,
            invalid_credential_response,
        )

        missing_argument_response = (  # type: ignore
            200,
            {},
            b'"Missing ISBN, UPC, or Client Identifier!"',
        )
        pytest.raises(
            Exception,
            novelist_fixture.novelist.review_response,
            missing_argument_response,
        )

        response = (200, {}, b"Here's the goods!")  # type: ignore
        assert response == novelist_fixture.novelist.review_response(response)

    def test_lookup_info_to_metadata(self, novelist_fixture: NoveListFixture):
        # Basic book information is returned
        identifier, ignore = Identifier.for_foreign_id(
            novelist_fixture.db.session, Identifier.ISBN, "9780804171335"
        )
        bad_character = novelist_fixture.sample_representation("a_bad_character.json")
        metadata = novelist_fixture.novelist.lookup_info_to_metadata(bad_character)

        assert True == isinstance(metadata, Metadata)
        assert Identifier.NOVELIST_ID == metadata.primary_identifier.type
        assert "10392078" == metadata.primary_identifier.identifier
        assert "A bad character" == metadata.title
        assert None == metadata.subtitle
        assert 1 == len(metadata.contributors)
        [contributor] = metadata.contributors
        assert "Kapoor, Deepti" == contributor.sort_name
        assert 4 == len(metadata.identifiers)
        assert 4 == len(metadata.subjects)
        assert 2 == len(metadata.measurements)
        ratings = sorted(metadata.measurements, key=lambda m: m.value)
        assert 2 == ratings[0].value
        assert 3.27 == ratings[1].value
        assert 625 == len(metadata.recommendations)

        # Confirm that Lexile and series data is extracted with a
        # different sample.
        vampire = novelist_fixture.sample_representation("vampire_kisses.json")
        metadata = novelist_fixture.novelist.lookup_info_to_metadata(vampire)

        [lexile] = filter(lambda s: s.type == "Lexile", metadata.subjects)
        assert "630" == lexile.identifier
        assert "Vampire kisses manga" == metadata.series
        # The full title should be selected, since every volume
        # has the same main title: 'Vampire kisses'
        assert "Vampire kisses: blood relatives. Volume 1" == metadata.title
        assert 1 == metadata.series_position
        assert 5 == len(metadata.recommendations)

    def test_get_series_information(self, novelist_fixture: NoveListFixture):
        metadata = Metadata(data_source=DataSource.NOVELIST)
        vampire = json.loads(novelist_fixture.sample_data("vampire_kisses.json"))
        book_info = vampire["TitleInfo"]
        series_info = vampire["FeatureContent"]["SeriesInfo"]

        (metadata, ideal_title_key) = novelist_fixture.novelist.get_series_information(
            metadata, series_info, book_info
        )
        # Relevant series information is extracted
        assert "Vampire kisses manga" == metadata.series
        assert 1 == metadata.series_position
        # The 'full_title' key should be returned as ideal because
        # all the volumes have the same 'main_title'
        assert "full_title" == ideal_title_key

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
        assert "Elvis Cole/Joe Pike novels" == metadata.series
        assert 11 == metadata.series_position
        # And recommends using the main_title
        assert "main_title" == ideal_title_key

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

        class Mock(NoveListAPI):
            def build_query_url(self, params):
                self.build_query_url_called_with = params
                return "http://query-url/"

            def scrubbed_url(self, params):
                self.scrubbed_url_called_with = params
                return "http://scrubbed-url/"

            def review_response(self, response):
                self.review_response_called_with = response

            def lookup_info_to_metadata(self, representation):
                self.lookup_info_to_metadata_called_with = representation
                return "some metadata"

        novelist = Mock.from_config(novelist_fixture.db.default_library())
        identifier = novelist_fixture.db.identifier(identifier_type=Identifier.ISBN)

        # Do the lookup.
        result = novelist.lookup(identifier, do_get=h.do_get)

        # A number of parameters were passed into build_query_url() to
        # get the URL of the HTTP request. The same parameters were
        # also passed into scrubbed_url(), to get the URL that should
        # be used when storing the Representation in the database.
        params1 = novelist.build_query_url_called_with
        params2 = novelist.scrubbed_url_called_with
        assert params1 == params2

        assert (
            dict(
                profile=novelist.profile,
                ClientIdentifier=identifier.urn,
                ISBN=identifier.identifier,
                password=novelist.password,
                version=novelist.version,
            )
            == params1
        )

        # The HTTP request went out to the query URL -- not the scrubbed URL.
        assert ["http://query-url/"] == h.requests

        # The HTTP response was passed into novelist.review_response()
        assert (
            200,
            {"content-type": "text/html"},
            b"yay",
        ) == novelist.review_response_called_with

        # Finally, the Representation was passed into
        # lookup_info_to_metadata, which returned a hard-coded string
        # as the final result.
        assert "some metadata" == result

        # Looking at the Representation we can see that it was stored
        # in the database under its scrubbed URL, not the URL used to
        # make the request.
        rep = novelist.lookup_info_to_metadata_called_with
        assert "http://scrubbed-url/" == rep.url
        assert b"yay" == rep.content

    def test_lookup_info_to_metadata_ignores_empty_responses(
        self, novelist_fixture: NoveListFixture
    ):
        """API requests that return no data result return a None tuple"""

        null_response = novelist_fixture.sample_representation("null_data.json")
        result = novelist_fixture.novelist.lookup_info_to_metadata(null_response)
        assert None == result

        # This also happens when NoveList indicates with an empty
        # response that it doesn't know the ISBN.
        empty_response = novelist_fixture.sample_representation("unknown_isbn.json")
        result = novelist_fixture.novelist.lookup_info_to_metadata(empty_response)
        assert None == result

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
        assert True == full_result.endswith(auth_details)
        assert "profile=username" in full_result
        assert "password=secret" in full_result

        # With a scrub, no authentication information is included.
        scrubbed_result = novelist_fixture.novelist.build_query_url(
            params, include_auth=False
        )
        assert False == scrubbed_result.endswith(auth_details)
        assert "profile=username" not in scrubbed_result
        assert "password=secret" not in scrubbed_result

        # Other details are urlencoded and available in both versions.
        for url in (scrubbed_result, full_result):
            assert "ClientIdentifier=C%20I" in url
            assert "ISBN=456" in url
            assert "version=2.2" in url

        # The method to create a scrubbed url returns the same result
        # as the NoveListAPI.build_query_url
        assert scrubbed_result == novelist_fixture.novelist.scrubbed_url(params)

    def test_scrub_subtitle(self, novelist_fixture: NoveListFixture):
        """Unnecessary title segments are removed from subtitles"""

        scrub = novelist_fixture.novelist._scrub_subtitle
        assert None == scrub(None)
        assert None == scrub("[electronic resource]")
        assert None == scrub("[electronic resource] :  ")
        assert "A Biomythography" == scrub("[electronic resource] :  A Biomythography")

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

        assert False == novelist_fixture.novelist._confirm_same_identifier(
            [metadata, mistake]
        )
        assert True == novelist_fixture.novelist._confirm_same_identifier(
            [metadata, match]
        )

    def test_lookup_equivalent_isbns(self, novelist_fixture: NoveListFixture):
        identifier = novelist_fixture.db.identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        api = MockNoveListAPI.from_config(novelist_fixture.db.default_library())

        # If there are no ISBN equivalents, it returns None.
        assert None == api.lookup_equivalent_isbns(identifier)

        source = DataSource.lookup(novelist_fixture.db.session, DataSource.OVERDRIVE)
        identifier.equivalent_to(source, novelist_fixture.db.identifier(), strength=1)
        novelist_fixture.db.session.commit()
        assert None == api.lookup_equivalent_isbns(identifier)

        # If there's an ISBN equivalent, but it doesn't result in metadata,
        # it returns none.
        isbn = novelist_fixture.db.identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn, strength=1)
        novelist_fixture.db.session.commit()
        api.responses.append(None)
        assert None == api.lookup_equivalent_isbns(identifier)

        # Create an API class that can mockout NoveListAPI.choose_best_metadata
        class MockBestMetadataAPI(MockNoveListAPI):
            choose_best_metadata_return = None

            def choose_best_metadata(self, *args, **kwargs):
                return self.choose_best_metadata_return

        api = MockBestMetadataAPI.from_config(novelist_fixture.db.default_library())

        # Give the identifier another ISBN equivalent.
        isbn2 = novelist_fixture.db.identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn2, strength=1)
        novelist_fixture.db.session.commit()

        # Queue metadata responses for each ISBN lookup.
        metadatas = [object(), object()]
        api.responses.extend(metadatas)

        # If choose_best_metadata returns None, the lookup returns None.
        api.choose_best_metadata_return = (None, None)
        assert None == api.lookup_equivalent_isbns(identifier)

        # Lookup was performed for both ISBNs.
        assert [] == api.responses

        # If choose_best_metadata returns a low confidence metadata, the
        # lookup returns None.
        api.responses.extend(metadatas)
        api.choose_best_metadata_return = (metadatas[0], 0.33)
        assert None == api.lookup_equivalent_isbns(identifier)

        # If choose_best_metadata returns a high confidence metadata, the
        # lookup returns the metadata.
        api.responses.extend(metadatas)
        api.choose_best_metadata_return = (metadatas[1], 0.67)
        assert metadatas[1] == api.lookup_equivalent_isbns(identifier)

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
        assert True == isinstance(result, tuple)
        assert metadatas[0] == result[0]
        # A default confidence of 1.0 is returned.
        assert 1.0 == result[1]

        # When top identifiers have equal representation, the method returns none.
        metadatas.append(
            Metadata(DataSource.NOVELIST, primary_identifier=less_identifier)
        )
        assert (None, None) == novelist_fixture.novelist.choose_best_metadata(
            metadatas, novelist_fixture.db.identifier()
        )

        # But when one pulls ahead, we get the metadata object again.
        metadatas.append(
            Metadata(DataSource.NOVELIST, primary_identifier=more_identifier)
        )
        result = novelist_fixture.novelist.choose_best_metadata(
            metadatas, novelist_fixture.db.identifier()
        )
        assert True == isinstance(result, tuple)
        metadata, confidence = result
        assert True == isinstance(metadata, Metadata)
        assert 0.67 == round(confidence, 2)
        assert more_identifier == metadata.primary_identifier

    def test_get_items_from_query(self, novelist_fixture: NoveListFixture):
        items = novelist_fixture.novelist.get_items_from_query(
            novelist_fixture.db.default_library()
        )
        # There are no books in the current library.
        assert items == []

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

        assert items == [item]

    def test_create_item_object(self, novelist_fixture: NoveListFixture):
        # We pass no identifier or item to process so we get nothing back.
        (
            currentIdentifier,
            existingItem,
            newItem,
            addItem,
        ) = novelist_fixture.novelist.create_item_object(None, None, None)
        assert currentIdentifier == None
        assert existingItem == None
        assert newItem == None
        assert addItem == False

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
        assert existingItem == None
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
        assert addItem == False

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
        assert newItem == None
        assert addItem == False

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
        assert newItem == None
        assert addItem == False

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
        assert addItem == True

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
        assert existingItem == None
        assert newItem == {
            "isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "role": "Narrator",
            "narrator": "Narrator 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101",
        }
        assert addItem == False

    def test_put_items_novelist(self, novelist_fixture: NoveListFixture):
        response = novelist_fixture.novelist.put_items_novelist(
            novelist_fixture.db.default_library()
        )

        assert response == None

        edition = novelist_fixture.db.edition(identifier_type=Identifier.ISBN)
        pool = novelist_fixture.db.licensepool(
            edition, collection=novelist_fixture.db.default_collection()
        )
        mock_response = {"Customer": "NYPL", "RecordsReceived": 10}

        def mockHTTPPut(url, headers, **kwargs):
            return MockRequestsResponse(200, content=json.dumps(mock_response))

        oldPut = novelist_fixture.novelist.put
        novelist_fixture.novelist.put = mockHTTPPut

        response = novelist_fixture.novelist.put_items_novelist(
            novelist_fixture.db.default_library()
        )

        assert response == mock_response

        novelist_fixture.novelist.put = oldPut

    def test_make_novelist_data_object(self, novelist_fixture: NoveListFixture):
        bad_data = []  # type: ignore
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

    def mockHTTPPut(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_put(self, novelist_fixture: NoveListFixture):
        oldPut = HTTP.put_with_timeout

        HTTP.put_with_timeout = self.mockHTTPPut  # type: ignore

        try:
            headers = {"AuthorizedIdentifier": "authorized!"}
            isbns = ["12345", "12346", "12347"]
            data = novelist_fixture.novelist.make_novelist_data_object(isbns)

            response = novelist_fixture.novelist.put(
                "http://apiendpoint.com", headers, data=data
            )
            (params, args) = self.called_with

            assert params == ("http://apiendpoint.com", data)
            assert args["headers"] == headers
        finally:
            HTTP.put_with_timeout = oldPut  # type: ignore
