from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest

from palace.util.datetime_helpers import datetime_utc

from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotHold,
    CirculationException,
    CurrentlyAvailable,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.integration.license.bibliotheca.api import BibliothecaAPI
from palace.manager.integration.license.bibliotheca.parser import (
    BibliothecaParser,
    CheckoutResponseParser,
    ErrorParser,
    EventParser,
    ItemListParser,
    PatronCirculationParser,
)
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from tests.manager.integration.license.bibliotheca.conftest import (
    BibliothecaAPITestFixture,
)

if TYPE_CHECKING:
    from tests.fixtures.files import BibliothecaFilesFixture


class TestBibliothecaParser:
    def test_parse_date(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        v = BibliothecaParser.parse_date("2016-01-02T12:34:56")
        assert v == datetime_utc(2016, 1, 2, 12, 34, 56)

        assert BibliothecaParser.parse_date(None) is None
        assert BibliothecaParser.parse_date("Some weird value") is None


class TestEventParser:
    def test_parse_empty_list(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("empty_event_batch.xml")

        # By default, we consider an empty batch of events not
        # as an error.
        events = list(EventParser().process_all(data))
        assert [] == events

        # But if we consider not having events for a certain time
        # period, then an exception should be raised.
        no_events_error = True
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            list(EventParser().process_all(data, no_events_error))
        assert (
            "No events returned from server. This may not be an error, but treating it as one to be safe."
            in str(excinfo.value)
        )

    @pytest.mark.parametrize("data", [b"", b"   \n  ", ""])
    def test_parse_empty_response_body(self, data: bytes | str):
        # Bibliotheca occasionally returns a completely empty response
        # body, which cannot be parsed as XML. We treat it the same as a
        # response containing no events rather than raising a parse error.
        events = list(EventParser().process_all(data))
        assert [] == events

        # And, as with a well-formed empty batch, we raise when the caller
        # has indicated that having no events should be treated as an error.
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            list(EventParser().process_all(data, no_events_error=True))
        assert (
            "No events returned from server. This may not be an error, but treating it as one to be safe."
            in str(excinfo.value)
        )

    def test_parse_empty_end_date_event(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        data = bibliotheca_fixture.files.sample_data("empty_end_date_event.xml")
        [event] = list(EventParser().process_all(data))
        (threem_id, isbn, patron_id, start_time, end_time, internal_event_type) = event
        assert "d5rf89" == threem_id
        assert "9781101190623" == isbn
        assert None == patron_id
        assert datetime_utc(2016, 4, 28, 11, 4, 6) == start_time
        assert None == end_time
        assert "distributor_license_add" == internal_event_type


class TestPatronCirculationParser:
    def test_parse(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("checkouts.xml")
        collection = bibliotheca_fixture.collection
        loans_and_holds = list(PatronCirculationParser(collection).process_all(data))
        loans = [x for x in loans_and_holds if isinstance(x, LoanInfo)]
        holds = [x for x in loans_and_holds if isinstance(x, HoldInfo)]
        assert 2 == len(loans)
        assert 2 == len(holds)
        [l1, l2] = sorted(loans, key=lambda x: str(x.identifier))
        assert "1ad589" == l1.identifier
        assert "cgaxr9" == l2.identifier
        expect_loan_start = datetime_utc(2015, 3, 20, 18, 50, 22)
        expect_loan_end = datetime_utc(2015, 4, 10, 18, 50, 22)
        assert expect_loan_start == l1.start_date
        assert expect_loan_end == l1.end_date

        [h1, h2] = sorted(holds, key=lambda x: str(x.identifier))

        # This is the book on reserve.
        assert collection.id == h1.collection_id
        assert "9wd8" == h1.identifier
        expect_hold_start = datetime_utc(2015, 5, 25, 17, 5, 34)
        expect_hold_end = datetime_utc(2015, 5, 27, 17, 5, 34)
        assert expect_hold_start == h1.start_date
        assert expect_hold_end == h1.end_date
        assert 0 == h1.hold_position

        # This is the book on hold.
        assert "d4o8r9" == h2.identifier
        assert collection.id == h2.collection_id
        expect_hold_start = datetime_utc(2015, 3, 24, 15, 6, 56)
        expect_hold_end = datetime_utc(2015, 3, 24, 15, 7, 51)
        assert expect_hold_start == h2.start_date
        assert expect_hold_end == h2.end_date
        assert 4 == h2.hold_position


class TestCheckoutResponseParser:
    def test_parse(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_first(data)
        assert datetime_utc(2015, 4, 16, 0, 32, 36) == due_date


class TestErrorParser:
    BIBLIOTHECA_ERROR_RESPONSE_BODY_TEMPLATE = (
        '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<Code>Gen-001</Code><Message>"
        "{message}"
        "</Message></Error>"
    )

    @pytest.mark.parametrize(
        "incoming_message, error_class, message, debug_message",
        [
            (
                "Patron cannot loan more than 12 documents",
                PatronLoanLimitReached,
                "Patron cannot loan more than 12 documents",
                None,
            ),
            (
                "Patron cannot have more than 15 holds",
                PatronHoldLimitReached,
                "Patron cannot have more than 15 holds",
                None,
            ),
            (
                "the patron document status was CAN_WISH and not one of CAN_LOAN,RESERVATION",
                NoLicenses,
                "The library currently has no licenses for this book.",
                "the patron document status was CAN_WISH and not one of CAN_LOAN,RESERVATION",
            ),
            (
                "the patron document status was CAN_HOLD and not one of CAN_LOAN,RESERVATION",
                NoAvailableCopies,
                "No copies available to check out.",
                "the patron document status was CAN_HOLD and not one of CAN_LOAN,RESERVATION",
            ),
            (
                "the patron document status was LOAN and not one of CAN_LOAN,RESERVATION",
                AlreadyCheckedOut,
                "You already have this book checked out.",
                "the patron document status was LOAN and not one of CAN_LOAN,RESERVATION",
            ),
            (
                "The patron has no eBooks checked out",
                NotCheckedOut,
                "The patron has no eBooks checked out",
                None,
            ),
            (
                "the patron document status was CAN_LOAN and not one of CAN_HOLD",
                CurrentlyAvailable,
                "Cannot place a hold on an available title.",
                "the patron document status was CAN_LOAN and not one of CAN_HOLD",
            ),
            (
                "the patron document status was HOLD and not one of CAN_HOLD",
                AlreadyOnHold,
                "You already have this book on hold.",
                "the patron document status was HOLD and not one of CAN_HOLD",
            ),
            (
                "The patron does not have the book on hold",
                NotOnHold,
                "The patron does not have the book on hold",
                None,
            ),
            # This is such a weird case we don't have a special exception for it.
            (
                "the patron document status was LOAN and not one of CAN_HOLD",
                CannotHold,
                "Could not place hold (reason unknown).",
                "the patron document status was LOAN and not one of CAN_HOLD",
            ),
        ],
    )
    def test_exception(
        self,
        incoming_message: str,
        error_class: type[CirculationException],
        message: str,
        debug_message: str | None,
    ):
        document = self.BIBLIOTHECA_ERROR_RESPONSE_BODY_TEMPLATE.format(
            message=incoming_message
        )
        error = ErrorParser().process_first(document)
        assert error.__class__ is error_class
        assert error.problem_detail.detail == message
        assert error.problem_detail.debug_message == debug_message

    @pytest.mark.parametrize(
        "incoming_message, incoming_message_from_file, error_string",
        [
            (
                # Simulate the message we get when the server goes down.
                "The server has encountered an error",
                None,
                "The server has encountered an error",
            ),
            (
                # Simulate an unexpected response, which is not a unicode string.
                b"Beep boop bytes",
                None,
                "Beep boop bytes",
            ),
            (
                # Simulate an unexpected response, which cannot be decoded as a string.
                b"\xde\xad\xbe\xef",
                None,
                "Unreadable error message (Unicode decode error).",
            ),
            (
                # Simulate the message we get when the server gives a vague error.
                None,
                "error_unknown.xml",
                "Unknown error",
            ),
            (
                # Simulate the message we get when the error message is
                # 'Authentication failed' but our authentication information is
                # set up correctly.
                None,
                "error_authentication_failed.xml",
                "Authentication failed",
            ),
            (
                """<weird>This error does not follow the standard set out by Bibliotheca.</weird>""",
                None,
                "Unknown error",
            ),
            (
                # Empty error message
                """<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Message/></Error>""",
                None,
                "Unknown error",
            ),
        ],
    )
    def test_remote_initiated_server_error(
        self,
        incoming_message: str | bytes | None,
        incoming_message_from_file: str | None,
        error_string: str,
        bibliotheca_files_fixture: BibliothecaFilesFixture,
    ):
        if incoming_message_from_file:
            incoming_message = bibliotheca_files_fixture.sample_text(
                incoming_message_from_file
            )
        assert incoming_message is not None
        error = ErrorParser().process_first(incoming_message)
        assert isinstance(error, RemoteInitiatedServerError)

        assert BibliothecaAPI.SERVICE_NAME == error.service_name
        assert error_string == str(error)

        problem = error.problem_detail
        assert 502 == problem.status_code
        assert "Integration error communicating with Bibliotheca" == problem.detail
        assert "Third-party service failed." == problem.title


class TestBibliothecaEventParser:
    # Sample event feed to test out the parser.
    TWO_EVENTS = """<LibraryEventBatch xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <PublishId>1b0d6667-a10e-424a-9f73-fb6f6d41308e</PublishId>
  <PublishDateTimeInUTC>2014-04-14T13:59:05.6920303Z</PublishDateTimeInUTC>
  <LastEventDateTimeInUTC>2014-04-03T00:00:34</LastEventDateTimeInUTC>
  <Events>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-1</EventId>
      <EventType>CHECKIN</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:23</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-03T00:00:23</EventEndDateTimeInUTC>
      <ItemId>theitem1</ItemId>
      <ISBN>900isbn1</ISBN>
      <PatronId>patronid1</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-2</EventId>
      <EventType>CHECKOUT</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:34</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-02T23:57:37</EventEndDateTimeInUTC>
      <ItemId>theitem2</ItemId>
      <ISBN>900isbn2</ISBN>
      <PatronId>patronid2</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
  </Events>
</LibraryEventBatch>
"""

    def test_parse_event_batch(self):
        # Parsing the XML gives us two events.
        event1, event2 = EventParser().process_all(self.TWO_EVENTS)

        (threem_id, isbn, patron_id, start_time, end_time, internal_event_type) = event1

        assert "theitem1" == threem_id
        assert "900isbn1" == isbn
        assert "patronid1" == patron_id
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == internal_event_type
        assert start_time == end_time

        (threem_id, isbn, patron_id, start_time, end_time, internal_event_type) = event2
        assert "theitem2" == threem_id
        assert "900isbn2" == isbn
        assert "patronid2" == patron_id
        assert CirculationEvent.DISTRIBUTOR_CHECKOUT == internal_event_type

        # Verify that start and end time were parsed correctly.
        correct_start = datetime_utc(2014, 4, 3, 0, 0, 34)
        correct_end = datetime_utc(2014, 4, 2, 23, 57, 37)
        assert correct_start == start_time
        assert correct_end == end_time


class TestItemListParser:
    def test_contributors_for_string(cls):
        authors = list(
            ItemListParser.contributors_from_string(
                "Walsh, Jill Paton; Sayers, Dorothy L."
            )
        )
        assert [x.sort_name for x in authors] == [
            "Walsh, Jill Paton",
            "Sayers, Dorothy L.",
        ]
        assert [x.roles for x in authors] == [
            (Contributor.Role.AUTHOR,),
            (Contributor.Role.AUTHOR,),
        ]

        # Parentheticals are stripped.
        [author] = ItemListParser.contributors_from_string(
            "Baum, Frank L. (Frank Lyell)"
        )
        assert "Baum, Frank L." == author.sort_name

        # Contributors may have two levels of entity reference escaping,
        # one of which will have already been handled by the initial parse.
        # So, we'll test zero and one escapings here.
        authors = list(
            ItemListParser.contributors_from_string(
                "Raji Codell, Esmé; Raji Codell, Esm&#233;"
            )
        )
        author_names = [a.sort_name for a in authors]
        assert len(authors) == 2
        assert len(set(author_names)) == 1
        assert all("Raji Codell, Esmé" == name for name in author_names)

        # It's possible to specify some role other than AUTHOR_ROLE.
        narrators = list(
            ItemListParser.contributors_from_string(
                "Callow, Simon; Mann, Bruce; Hagon, Garrick", Contributor.Role.NARRATOR
            )
        )
        for narrator in narrators:
            assert (Contributor.Role.NARRATOR,) == narrator.roles
        assert ["Callow, Simon", "Mann, Bruce", "Hagon, Garrick"] == [
            narrator.sort_name for narrator in narrators
        ]

    def test_parse_genre_string(self):
        def f(genre_string):
            genres = ItemListParser.parse_genre_string(genre_string)
            assert all([x.type == Subject.BISAC for x in genres])
            return [x.name for x in genres]

        assert ["Children's Health", "Health"] == f("Children&amp;#39;s Health,Health,")

        assert [
            "Action & Adventure",
            "Science Fiction",
            "Fantasy",
            "Magic",
            "Renaissance",
        ] == f(
            "Action &amp;amp; Adventure,Science Fiction, Fantasy, Magic,Renaissance,"
        )

    def test_item_list(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("item_metadata_list_mini.xml")
        data_parsed = list(ItemListParser().process_all(data))

        # There should be 2 items in the list.
        assert 2 == len(data_parsed)

        cooked = data_parsed[0]

        assert "The Incense Game" == cooked.title
        assert "A Novel of Feudal Japan" == cooked.subtitle
        assert Edition.BOOK_MEDIUM == cooked.medium
        assert "eng" == cooked.language
        assert "St. Martin's Press" == cooked.publisher
        assert date(year=2012, month=9, day=17) == cooked.published

        primary = cooked.primary_identifier_data
        assert "ddf4gr9" == primary.identifier
        assert Identifier.BIBLIOTHECA_ID == primary.type

        identifiers = sorted(cooked.identifiers, key=lambda x: x.identifier)
        assert ["9781250015280", "9781250031112", "ddf4gr9"] == [
            x.identifier for x in identifiers
        ]

        [author] = cooked.contributors
        assert "Rowland, Laura Joh" == author.sort_name
        assert (Contributor.Role.AUTHOR,) == author.roles

        subjects = [x.name for x in cooked.subjects if x.name is not None]
        assert ["Children's Health", "Mystery & Detective"] == sorted(subjects)

        [pages] = cooked.measurements
        assert Measurement.PAGE_COUNT == pages.quantity_measured
        assert 304 == pages.value

        [alternate, image, description] = sorted(cooked.links, key=lambda x: x.rel)
        assert "alternate" == alternate.rel
        assert alternate.href.startswith("http://ebook.3m.com/library")

        # We have a full-size image...
        assert Hyperlink.IMAGE == image.rel
        assert Representation.JPEG_MEDIA_TYPE == image.media_type
        assert image.href is not None
        assert image.href.startswith("http://ebook.3m.com/delivery")
        assert "documentID=ddf4gr9" in image.href
        assert "&size=NORMAL" not in image.href

        # ... and a thumbnail, which we obtained by adding an argument
        # to the main image URL.
        thumbnail = image.thumbnail
        assert Hyperlink.THUMBNAIL_IMAGE == thumbnail.rel
        assert Representation.JPEG_MEDIA_TYPE == thumbnail.media_type
        assert thumbnail.href == image.href + "&size=NORMAL"

        # We have a description.
        assert Hyperlink.DESCRIPTION == description.rel
        assert isinstance(description.content, str)
        assert description.content.startswith("<b>Winner")

    def test_multiple_contributor_roles(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        data = bibliotheca_fixture.files.sample_data("item_metadata_audio.xml")
        [parsed_data] = list(ItemListParser().process_all(data))
        names_and_roles = []
        for c in parsed_data.contributors:
            [role] = c.roles
            names_and_roles.append((c.sort_name, role))

        # We found one author and three narrators.
        assert sorted(
            [
                ("Riggs, Ransom", "Author"),
                ("Callow, Simon", "Narrator"),
                ("Mann, Bruce", "Narrator"),
                ("Hagon, Garrick", "Narrator"),
            ]
        ) == sorted(names_and_roles)

    def test_circulation_data_status(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        """Test that CirculationData from ItemListParser has correct status."""
        data = bibliotheca_fixture.files.sample_data("item_metadata_list_mini.xml")
        data_parsed = list(ItemListParser().process_all(data))

        # Check the first book's circulation data
        bibliographic1 = data_parsed[0]
        circulation1 = bibliographic1.circulation

        # This book has 1 license, so status should be ACTIVE
        assert circulation1.licenses_owned == 1
        assert circulation1.licenses_available == 1
        assert circulation1.status == LicensePoolStatus.ACTIVE

        # Check the second book's circulation data
        bibliographic2 = data_parsed[1]
        circulation2 = bibliographic2.circulation

        # This book also has licenses, so status should be ACTIVE
        assert circulation2.licenses_owned == 1
        assert circulation2.status == LicensePoolStatus.ACTIVE

    def test_circulation_data_status_exhausted(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        """Test that CirculationData has EXHAUSTED status when licenses_owned is 0."""
        data = bibliotheca_fixture.files.sample_data("item_metadata_list_mini.xml")
        # Replace TotalCopies with 0 to test EXHAUSTED status
        data = data.replace(
            b"<TotalCopies>1</TotalCopies>", b"<TotalCopies>0</TotalCopies>"
        )

        data_parsed = list(ItemListParser().process_all(data))

        # Both books should have EXHAUSTED status
        for bibliographic in data_parsed:
            circulation = bibliographic.circulation
            assert circulation.licenses_owned == 0
            assert circulation.status == LicensePoolStatus.EXHAUSTED

    def test_internal_formats(self):
        m = ItemListParser.internal_formats

        def _check_format(input, expect_medium, expect_format, expect_drm):
            medium, formats = m(input)
            assert medium == expect_medium
            [format] = formats
            assert expect_format == format.content_type
            assert expect_drm == format.drm_scheme

        rep = Representation
        adobe = DeliveryMechanism.ADOBE_DRM
        findaway = DeliveryMechanism.FINDAWAY_DRM
        book = Edition.BOOK_MEDIUM

        # Verify that we handle the known strings from Bibliotheca
        # appropriately.
        _check_format("EPUB", book, rep.EPUB_MEDIA_TYPE, adobe)
        _check_format("EPUB3", book, rep.EPUB_MEDIA_TYPE, adobe)
        _check_format("PDF", book, rep.PDF_MEDIA_TYPE, adobe)
        _check_format("MP3", Edition.AUDIO_MEDIUM, None, findaway)

        # Now Try a string we don't recognize from Bibliotheca.
        medium, formats = m("Unknown")

        # We assume it's a book.
        assert Edition.BOOK_MEDIUM == medium

        # But we don't know which format.
        assert [] == formats
