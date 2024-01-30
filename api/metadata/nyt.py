from __future__ import annotations

from core.selftest import HasSelfTests, SelfTestResult

"""Interface to the New York Times APIs."""
import json
import sys
from collections.abc import Generator
from datetime import date, datetime, timedelta
from typing import Any

from dateutil import tz
from sqlalchemy import select
from sqlalchemy.orm.session import Session

from api.config import CannotLoadConfiguration, IntegrationException
from api.metadata.base import MetadataService, MetadataServiceSettings
from core.external_list import TitleFromExternalList
from core.integration.goals import Goals
from core.integration.settings import ConfigurationFormItem, FormField
from core.metadata_layer import ContributorData, IdentifierData, Metadata
from core.model import (
    CustomList,
    DataSource,
    Edition,
    Identifier,
    IntegrationConfiguration,
    Representation,
    get_one_or_create,
)
from core.util.log import LoggerMixin

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class NytBestSellerApiSettings(MetadataServiceSettings):
    password: str = FormField(
        ...,
        form=ConfigurationFormItem(
            label="API key",
        ),
    )


class NYTAPI:
    DATE_FORMAT = "%Y-%m-%d"

    # NYT best-seller lists are associated with dates, but fields like
    # CustomEntry.first_appearance are timezone-aware datetimes. We
    # will interpret a date as meaning midnight of that day in New
    # York.
    #
    # NOTE: entries fetched before we made the datetimes
    # timezone-aware will have their time zones set to UTC, but the
    # difference is negligible.
    TIME_ZONE = tz.gettz("America/New York")

    @classmethod
    def parse_datetime(cls, d: str) -> datetime:
        """Used to parse the publication date of a NYT best-seller list.

        We take midnight Eastern time to be the publication time.
        """
        return datetime.strptime(d, cls.DATE_FORMAT).replace(tzinfo=cls.TIME_ZONE)

    @classmethod
    def parse_date(cls, d: str) -> date:
        """Used to parse the publication date of a book.

        We don't know the timezone here, so the date will end up being
        stored as midnight UTC.
        """
        return cls.parse_datetime(d).date()

    @classmethod
    def date_string(cls, d: date) -> str:
        return d.strftime(cls.DATE_FORMAT)


class NYTBestSellerAPI(
    NYTAPI,
    MetadataService[NytBestSellerApiSettings],
    HasSelfTests,
    LoggerMixin,
):
    BASE_URL = "http://api.nytimes.com/svc/books/v3/lists"

    LIST_NAMES_URL = BASE_URL + "/names.json"
    LIST_URL = BASE_URL + ".json?list=%s"

    LIST_OF_LISTS_MAX_AGE = timedelta(days=1)
    LIST_MAX_AGE = timedelta(days=1)
    HISTORICAL_LIST_MAX_AGE = timedelta(days=365)

    @classmethod
    def label(cls) -> str:
        return "NYT Best Seller API"

    @classmethod
    def description(cls) -> str:
        return ""

    @classmethod
    def settings_class(cls) -> type[NytBestSellerApiSettings]:
        return NytBestSellerApiSettings

    @classmethod
    def integration(cls, _db: Session) -> IntegrationConfiguration | None:
        query = select(IntegrationConfiguration).where(
            IntegrationConfiguration.goal == Goals.METADATA_GOAL,
            IntegrationConfiguration.protocol.in_(cls.protocols()),
        )
        return _db.execute(query).scalar_one_or_none()

    @classmethod
    def from_config(cls, _db: Session) -> Self:
        integration = cls.integration(_db)

        if not integration:
            message = "No Integration found for the NYT."
            raise CannotLoadConfiguration(message)

        settings = cls.settings_load(integration)
        return cls(_db, settings=settings)

    def __init__(self, _db: Session, settings: NytBestSellerApiSettings) -> None:
        self._db = _db
        self.api_key = settings.password

    @classmethod
    def do_get(
        cls, url: str, headers: dict[str, str], **kwargs: Any
    ) -> tuple[int, dict[str, str], bytes]:
        return Representation.simple_http_get(url, headers, **kwargs)

    @classmethod
    def multiple_services_allowed(cls) -> bool:
        return False

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult, None, None]:
        yield self.run_test("Getting list of best-seller lists", self.list_of_lists)

    def request(self, path: str, max_age: timedelta = LIST_MAX_AGE) -> dict[str, Any]:
        if not path.startswith(self.BASE_URL):
            if not path.startswith("/"):
                path = "/" + path
            url = self.BASE_URL + path
        else:
            url = path
        joiner = "?"
        if "?" in url:
            joiner = "&"
        url += joiner + "api-key=" + self.api_key
        representation, cached = Representation.get(
            self._db,
            url,
            do_get=self.do_get,
            max_age=max_age,
            debug=True,
            pause_before=0.1,
        )
        status = representation.status_code
        if status == 200:
            # Everything's fine.
            content = json.loads(representation.content)
            return content  # type: ignore[no-any-return]

        diagnostic = "Response from {} was: {!r}".format(
            url,
            representation.content.decode("utf-8") if representation.content else "",
        )

        if status == 403:
            raise IntegrationException(
                "API authentication failed",
                "API key is most likely wrong. %s" % diagnostic,
            )
        else:
            raise IntegrationException(
                "Unknown API error (status %s)" % status, diagnostic
            )

    def list_of_lists(self, max_age: timedelta = LIST_MAX_AGE) -> dict[str, Any]:
        return self.request(self.LIST_NAMES_URL, max_age=max_age)

    def list_info(self, list_name: str) -> dict[str, Any]:
        list_of_lists = self.list_of_lists()
        list_info = [
            x for x in list_of_lists["results"] if x["list_name_encoded"] == list_name
        ]
        if not list_info:
            raise ValueError("No such list: %s" % list_name)
        return list_info[0]  # type: ignore[no-any-return]

    def best_seller_list(self, list_info: str | dict[str, Any]) -> NYTBestSellerList:
        """Create (but don't update) a NYTBestSellerList object."""
        if isinstance(list_info, str):
            list_info = self.list_info(list_info)
        return NYTBestSellerList(list_info)

    def update(
        self,
        list: NYTBestSellerList,
        date: date | None = None,
        max_age: timedelta = LIST_MAX_AGE,
    ) -> None:
        """Update the given list with data from the given date."""
        name = list.foreign_identifier
        url = self.LIST_URL % name
        if date:
            url += "&published-date=%s" % self.date_string(date)

        data = self.request(url, max_age=max_age)
        list.update(data)

    def fill_in_history(self, list: NYTBestSellerList) -> None:
        """Update the given list with current and historical data."""
        for date in list.all_dates:
            self.update(list, date, self.HISTORICAL_LIST_MAX_AGE)
            self._db.commit()


class NYTBestSellerList(list["NYTBestSellerListTitle"], LoggerMixin):
    def __init__(self, list_info: dict[str, Any]) -> None:
        self.name = list_info["display_name"]
        self.created = NYTAPI.parse_datetime(list_info["oldest_published_date"])
        self.updated = NYTAPI.parse_datetime(list_info["newest_published_date"])
        self.foreign_identifier = list_info["list_name_encoded"]
        if list_info["updated"] == "WEEKLY":
            frequency = 7
        elif list_info["updated"] == "MONTHLY":
            frequency = 30
        self.frequency = timedelta(frequency)
        self.items_by_isbn: dict[str, NYTBestSellerListTitle] = dict()

    @property
    def medium(self) -> str | None:
        """What medium are the books on this list?

        Lists like "Audio Fiction" contain audiobooks; all others
        contain normal books. (TODO: this isn't quite right; the
        distinction between ebooks and print books here exists in a
        way it doesn't with most other sources of Editions.)
        """
        name = self.name
        if not name:
            return None
        if name.startswith("Audio "):
            return Edition.AUDIO_MEDIUM
        return Edition.BOOK_MEDIUM

    @property
    def all_dates(self) -> Generator[datetime, None, None]:
        """Yield a list of estimated dates when new editions of this list were
        probably published.
        """
        date = self.updated
        end = self.created
        while date >= end:
            yield date
            old_date = date
            date = date - self.frequency
            if old_date > end and date < end:
                # We overshot the end date.
                yield end

    def update(self, json_data: dict[str, Any]) -> None:
        """Update the list with information from the given JSON structure."""
        for li_data in json_data.get("results", []):
            try:
                book = li_data["book_details"][0]
                key = book.get("primary_isbn13") or book.get("primary_isbn10")
                if key in self.items_by_isbn:
                    item = self.items_by_isbn[key]
                    self.log.debug("Previously seen ISBN: %r", key)
                else:
                    item = NYTBestSellerListTitle(li_data, self.medium)
                    self.items_by_isbn[key] = item
                    self.append(item)
                    # self.log.debug("Newly seen ISBN: %r, %s", key, len(self))
            except ValueError:
                # Should only happen when the book has no identifier, which...
                # should never happen.
                self.log.error("No identifier for %r", li_data)
                item = None

            if item is None:
                continue

            # This is the date the *best-seller list* was published,
            # not the date the book was published.
            list_date = NYTAPI.parse_datetime(li_data["published_date"])
            if not item.first_appearance or list_date < item.first_appearance:
                item.first_appearance = list_date
            if (
                not item.most_recent_appearance
                or list_date > item.most_recent_appearance
            ):
                item.most_recent_appearance = list_date

    def to_customlist(self, _db: Session) -> CustomList:
        """Turn this NYTBestSeller list into a CustomList object."""
        data_source = DataSource.lookup(_db, DataSource.NYT)
        l, was_new = get_one_or_create(
            _db,
            CustomList,
            data_source=data_source,
            foreign_identifier=self.foreign_identifier,
            create_method_kwargs=dict(
                created=self.created,
            ),
        )
        l.name = self.name
        l.updated = self.updated
        self.update_custom_list(l)
        return l

    def update_custom_list(self, custom_list: CustomList) -> None:
        """Make sure the given CustomList's CustomListEntries reflect
        the current state of the NYTBestSeller list.
        """
        db = Session.object_session(custom_list)

        # Add new items to the list.
        for i in self:
            list_item, was_new = i.to_custom_list_entry(custom_list)
            # If possible, associate the item with a Work.
            list_item.set_work()


class NYTBestSellerListTitle(TitleFromExternalList):
    def __init__(self, data: dict[str, Any], medium: str | None) -> None:
        try:
            bestsellers_date = NYTAPI.parse_datetime(data.get("bestsellers_date"))  # type: ignore[arg-type]
            first_appearance = bestsellers_date
            most_recent_appearance = bestsellers_date
        except ValueError as e:
            first_appearance = None
            most_recent_appearance = None

        try:
            # This is the date the _book_ was published, not the date
            # the _bestseller list_ was published.
            published_date = NYTAPI.parse_date(data.get("published_date"))  # type: ignore[arg-type]
        except ValueError as e:
            published_date = None

        details = data["book_details"]
        other_isbns = []
        if len(details) == 0:
            publisher = annotation = primary_isbn10 = primary_isbn13 = title = None
            display_author = None
        else:
            d = details[0]
            title = d.get("title", None)
            display_author = d.get("author", None)
            publisher = d.get("publisher", None)
            annotation = d.get("description", None)
            primary_isbn10 = d.get("primary_isbn10", None)
            primary_isbn13 = d.get("primary_isbn13", None)

            # The list of other ISBNs frequently contains ISBNs for
            # other books in the same series, as well as ISBNs that
            # are just wrong. Assign these equivalencies at a low
            # level of confidence.
            for isbn in d.get("isbns", []):
                isbn13 = isbn.get("isbn13", None)
                if isbn13:
                    other_isbns.append(IdentifierData(Identifier.ISBN, isbn13, 0.50))

        primary_isbn = primary_isbn13 or primary_isbn10
        if primary_isbn:
            primary_isbn = IdentifierData(Identifier.ISBN, primary_isbn, 0.90)

        contributors = []
        if display_author:
            contributors.append(ContributorData(display_name=display_author))

        metadata = Metadata(
            data_source=DataSource.NYT,
            title=title,
            medium=medium,
            language="eng",
            published=published_date,
            publisher=publisher,
            contributors=contributors,
            primary_identifier=primary_isbn,
            identifiers=other_isbns,
        )

        super().__init__(metadata, first_appearance, most_recent_appearance, annotation)
