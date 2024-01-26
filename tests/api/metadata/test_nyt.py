import datetime
import json
from unittest.mock import MagicMock

import pytest

from api.integration.registry.metadata import MetadataRegistry
from api.metadata.nyt import (
    NYTAPI,
    NYTBestSellerAPI,
    NytBestSellerApiSettings,
    NYTBestSellerList,
    NYTBestSellerListTitle,
)
from core.config import CannotLoadConfiguration
from core.integration.goals import Goals
from core.model import Contributor, CustomListEntry, Edition
from core.util.http import IntegrationException
from tests.fixtures.api_nyt_files import NYTFilesFixture
from tests.fixtures.database import DatabaseTransactionFixture


class DummyNYTBestSellerAPI(NYTBestSellerAPI):
    def __init__(self, _db, files: NYTFilesFixture):
        self._db = _db
        self.files = files

    def sample_json(self, filename):
        return json.loads(self.files.sample_data(filename))

    def list_of_lists(self):
        return self.sample_json("bestseller_list_list.json")

    def update(self, list, date=None, max_age=None):
        if date:
            filename = "list_{}_{}.json".format(
                list.foreign_identifier,
                self.date_string(date),
            )
        else:
            filename = "list_%s.json" % list.foreign_identifier
        list.update(self.sample_json(filename))


class NYTBestSellerAPIFixture:
    def midnight(self, *args):
        """Create a datetime representing midnight Eastern time (the time we
        take NYT best-seller lists to be published) on a certain date.
        """
        return datetime.datetime(*args, tzinfo=NYTAPI.TIME_ZONE)

    def protocol(self) -> str:
        registry = MetadataRegistry()
        protocol = registry.get_protocol(NYTBestSellerAPI)
        assert protocol is not None
        return protocol

    def __init__(self, db: DatabaseTransactionFixture, files: NYTFilesFixture):
        self.db = db
        self.api = DummyNYTBestSellerAPI(db.session, files)


@pytest.fixture(scope="function")
def nyt_fixture(
    db: DatabaseTransactionFixture, api_nyt_files_fixture: NYTFilesFixture
) -> NYTBestSellerAPIFixture:
    return NYTBestSellerAPIFixture(db, api_nyt_files_fixture)


class TestNYTBestSellerAPI:

    """Test the API calls."""

    def test_from_config(self, nyt_fixture: NYTBestSellerAPIFixture):
        # You have to have an ExternalIntegration for the NYT.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            NYTBestSellerAPI.from_config(nyt_fixture.db.session)
        assert "No Integration found for the NYT." in str(excinfo.value)

        integration = nyt_fixture.db.integration_configuration(
            protocol=nyt_fixture.protocol(), goal=Goals.METADATA_GOAL
        )
        settings = NytBestSellerApiSettings(password="api key")
        NYTBestSellerAPI.settings_update(integration, settings)

        api = NYTBestSellerAPI.from_config(nyt_fixture.db.session)
        assert "api key" == api.api_key

        # integration() finds the integration used to create the API object.
        assert integration == api.integration(nyt_fixture.db.session)

    def test_run_self_tests(self, nyt_fixture: NYTBestSellerAPIFixture):
        class Mock(NYTBestSellerAPI):
            def __init__(self):
                pass

            def list_of_lists(self):
                return "some lists"

        [list_test] = Mock()._run_self_tests(MagicMock())
        assert "Getting list of best-seller lists" == list_test.name
        assert list_test.success is True
        assert "some lists" == list_test.result

    def test_list_of_lists(self, nyt_fixture: NYTBestSellerAPIFixture):
        all_lists = nyt_fixture.api.list_of_lists()
        assert ["copyright", "num_results", "results", "status"] == sorted(
            all_lists.keys()
        )
        assert 47 == len(all_lists["results"])

    def test_list_info(self, nyt_fixture: NYTBestSellerAPIFixture):
        list_info = nyt_fixture.api.list_info("combined-print-and-e-book-fiction")
        assert "Combined Print & E-Book Fiction" == list_info["display_name"]

    def test_request_failure(self, nyt_fixture: NYTBestSellerAPIFixture):
        # Verify that certain unexpected HTTP results are turned into
        # IntegrationExceptions.

        nyt_fixture.api.api_key = "some key"

        def result_403(*args, **kwargs):
            return 403, None, None

        nyt_fixture.api.do_get = result_403
        with pytest.raises(IntegrationException) as excinfo:
            nyt_fixture.api.request("some path")
        assert "API authentication failed" in str(excinfo.value)

        def result_500(*args, **kwargs):
            return 500, {}, "bad value"

        nyt_fixture.api.do_get = result_500
        try:
            nyt_fixture.api.request("some path")
            raise Exception("Expected an IntegrationException!")
        except IntegrationException as e:
            assert "Unknown API error (status 500)" == str(e)
            assert e.debug_message.startswith("Response from")
            assert e.debug_message.endswith("was: 'bad value'")


class TestNYTBestSellerList:

    """Test the NYTBestSellerList object and its ability to be turned
    into a CustomList.
    """

    def test_creation(self, nyt_fixture: NYTBestSellerAPIFixture):
        # Just creating a list doesn't add any items to it.
        list_name = "combined-print-and-e-book-fiction"
        l = nyt_fixture.api.best_seller_list(list_name)
        assert True == isinstance(l, NYTBestSellerList)
        assert 0 == len(l)

    def test_medium(self, nyt_fixture: NYTBestSellerAPIFixture):
        list_name = "combined-print-and-e-book-fiction"
        l = nyt_fixture.api.best_seller_list(list_name)
        assert "Combined Print & E-Book Fiction" == l.name
        assert Edition.BOOK_MEDIUM == l.medium

        l.name = "Audio Nonfiction"
        assert Edition.AUDIO_MEDIUM == l.medium

    def test_update(self, nyt_fixture: NYTBestSellerAPIFixture):
        list_name = "combined-print-and-e-book-fiction"
        l = nyt_fixture.api.best_seller_list(list_name)
        nyt_fixture.api.update(l)

        assert 20 == len(l)
        assert True == all([isinstance(x, NYTBestSellerListTitle) for x in l])
        assert nyt_fixture.midnight(2011, 2, 13) == l.created
        assert nyt_fixture.midnight(2015, 2, 1) == l.updated
        assert list_name == l.foreign_identifier

        # Let's do a spot check on the list items.
        title = [x for x in l if x.metadata.title == "THE GIRL ON THE TRAIN"][0]
        [isbn] = title.metadata.identifiers
        assert "ISBN" == isbn.type
        assert "9780698185395" == isbn.identifier

        # The list's medium is propagated to its Editions.
        assert l.medium == title.metadata.medium

        [contributor] = title.metadata.contributors
        assert "Paula Hawkins" == contributor.display_name
        assert "Riverhead" == title.metadata.publisher
        assert (
            "A psychological thriller set in London is full of complications and betrayals."
            == title.annotation
        )
        assert nyt_fixture.midnight(2015, 1, 17) == title.first_appearance
        assert nyt_fixture.midnight(2015, 2, 1) == title.most_recent_appearance

    def test_historical_dates(self, nyt_fixture: NYTBestSellerAPIFixture):
        # This list was published 208 times since the start of the API,
        # and we can figure out when.

        list_name = "combined-print-and-e-book-fiction"
        l = nyt_fixture.api.best_seller_list(list_name)
        dates = list(l.all_dates)
        assert 208 == len(dates)
        assert l.updated == dates[0]
        assert l.created == dates[-1]

    def test_to_customlist(self, nyt_fixture: NYTBestSellerAPIFixture):
        list_name = "combined-print-and-e-book-fiction"
        l = nyt_fixture.api.best_seller_list(list_name)
        nyt_fixture.api.update(l)
        custom = l.to_customlist(nyt_fixture.db.session)
        assert custom.created == l.created
        assert custom.updated == l.updated
        assert custom.name == l.name
        assert len(l) == len(custom.entries)
        assert True == all([isinstance(x, CustomListEntry) for x in custom.entries])

        assert 20 == len(custom.entries)

        # The publication of a NYT best-seller list is treated as
        # midnight Eastern time on the publication date.
        jan_17 = nyt_fixture.midnight(2015, 1, 17)
        assert True == all([x.first_appearance == jan_17 for x in custom.entries])

        feb_1 = nyt_fixture.midnight(2015, 2, 1)
        assert True == all([x.most_recent_appearance == feb_1 for x in custom.entries])

        # Now replace this list's entries with the entries from a
        # different list. We wouldn't do this in real life, but it's
        # a convenient way to change the contents of a list.
        other_nyt_list = nyt_fixture.api.best_seller_list("hardcover-fiction")
        nyt_fixture.api.update(other_nyt_list)
        other_nyt_list.update_custom_list(custom)

        # The CustomList now contains elements from both NYT lists.
        assert 40 == len(custom.entries)

    def test_fill_in_history(self, nyt_fixture: NYTBestSellerAPIFixture):
        list_name = "espionage"
        l = nyt_fixture.api.best_seller_list(list_name)
        nyt_fixture.api.fill_in_history(l)

        # Each 'espionage' best-seller list contains 15 items. Since
        # we picked two, from consecutive months, there's quite a bit
        # of overlap, and we end up with 20.
        assert 20 == len(l)


class TestNYTBestSellerListTitle:
    one_list_title = json.loads(
        r"""{"list_name":"Combined Print and E-Book Fiction","display_name":"Combined Print & E-Book Fiction","bestsellers_date":"2015-01-17","published_date":"2015-02-01","rank":1,"rank_last_week":0,"weeks_on_list":1,"asterisk":0,"dagger":0,"amazon_product_url":"http:\/\/www.amazon.com\/The-Girl-Train-A-Novel-ebook\/dp\/B00L9B7IKE?tag=thenewyorktim-20","isbns":[{"isbn10":"1594633665","isbn13":"9781594633669"},{"isbn10":"0698185390","isbn13":"9780698185395"}],"book_details":[{"title":"THE GIRL ON THE TRAIN","description":"A psychological thriller set in London is full of complications and betrayals.","contributor":"by Paula Hawkins","author":"Paula Hawkins","contributor_note":"","price":0,"age_group":"","publisher":"Riverhead","isbns":[{"isbn10":"1594633665","isbn13":"9781594633669"},{"isbn10":"0698185390","isbn13":"9780698185395"}],"primary_isbn13":"9780698185395","primary_isbn10":"0698185390"}],"reviews":[{"book_review_link":"","first_chapter_link":"","sunday_review_link":"","article_chapter_link":""}]}"""
    )

    def test_creation(self, nyt_fixture: NYTBestSellerAPIFixture):
        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)

        edition = title.to_edition(nyt_fixture.db.session)
        assert "9780698185395" == edition.primary_identifier.identifier

        # The alternate ISBN is marked as equivalent to the primary identifier,
        # but at a greatly reduced strength.
        [equivalency] = [x for x in edition.primary_identifier.equivalencies]
        assert "9781594633669" == equivalency.output.identifier
        assert 0.5 == equivalency.strength
        # That strength is not enough to make the alternate ISBN an equivalent
        # identifier for the edition.
        equivalent_identifiers = [
            (x.type, x.identifier) for x in edition.equivalent_identifiers()
        ]
        assert [("ISBN", "9780698185395")] == sorted(equivalent_identifiers)

        assert datetime.date(2015, 2, 1) == edition.published
        assert "Paula Hawkins" == edition.author
        assert "Hawkins, Paula" == edition.sort_author
        assert "Riverhead" == edition.publisher

    def test_to_edition_sets_sort_author_name_if_obvious(
        self, nyt_fixture: NYTBestSellerAPIFixture
    ):
        [contributor], ignore = Contributor.lookup(
            nyt_fixture.db.session, "Hawkins, Paula"
        )
        contributor.display_name = "Paula Hawkins"

        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)
        edition = title.to_edition(nyt_fixture.db.session)
        assert contributor.sort_name == edition.sort_author
        assert contributor.display_name == edition.author
        assert edition.permanent_work_id is not None
