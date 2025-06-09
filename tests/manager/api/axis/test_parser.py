from __future__ import annotations

import datetime
import json

import pytest

from palace.manager.api.axis.fulfillment import (
    Axis360AcsFulfillment,
    Axis360Fulfillment,
)
from palace.manager.api.axis.manifest import AxisNowManifest
from palace.manager.api.axis.parser import (
    AudiobookMetadataParser,
    AvailabilityResponseParser,
    Axis360FulfillmentInfoResponseParser,
    BibliographicParser,
    CheckinResponseParser,
    CheckoutResponseParser,
    HoldReleaseResponseParser,
    HoldResponseParser,
    JSONResponseParser,
    StatusResponseParser,
)
from palace.manager.api.circulation import HoldInfo, LoanInfo, UrlFulfillment
from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    NotFoundOnRemote,
    NotOnHold,
    PatronAuthorizationFailedException,
    RemoteInitiatedServerError,
)
from palace.manager.api.web_publication_manifest import FindawayManifest, SpineItem
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import AxisFilesFixture
from tests.manager.api.axis.conftest import Axis360Fixture


class TestStatusResponseParser:
    def test_status_parser(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("availability_patron_not_found.xml")
        parser = StatusResponseParser()
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 3122
        assert message == "Patron information is not found."

        data = axis_files_fixture.sample_data("availability_with_loans.xml")
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 0
        assert message == "Availability Data is Successfully retrieved."

        data = axis_files_fixture.sample_data("availability_with_ebook_fulfillment.xml")
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 0
        assert message == "Availability Data is Successfully retrieved."

        data = axis_files_fixture.sample_data("checkin_failure.xml")
        parsed = parser.process_first(data)
        assert parsed is not None
        status, message = parsed
        assert status == 3103
        assert message == "Invalid Title Id"

        data = axis_files_fixture.sample_data("invalid_error_code.xml")
        assert parser.process_first(data) is None

        data = axis_files_fixture.sample_data("missing_error_code.xml")
        assert parser.process_first(data) is None
        assert parser.process_first(None) is None
        assert parser.process_first(b"") is None
        assert parser.process_first(b"not xml") is None


class TestBibliographicParser:
    def test_bibliographic_parser(self, axis_files_fixture: AxisFilesFixture):
        # Make sure the bibliographic information gets properly
        # collated in preparation for creating Edition objects.

        data = axis_files_fixture.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser().process_all(data)

        # We test for availability information in a separate test.
        # Here we just make sure it is present.
        assert av1 is not None
        assert av2 is not None

        # But we did get bibliographic information.
        assert bib1 is not None
        assert bib2 is not None

        assert "Faith of My Fathers : A Family Memoir" == bib1.title
        assert "eng" == bib1.language
        assert datetime.date(2000, 3, 7) == bib1.published

        assert "Simon & Schuster" == bib2.publisher
        assert "Pocket Books" == bib2.imprint

        assert Edition.BOOK_MEDIUM == bib1.medium

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        assert None == bib2.series

        # Book #1 has two links -- a description and a cover image.
        [description, cover] = bib1.links
        assert Hyperlink.DESCRIPTION == description.rel
        assert Representation.TEXT_PLAIN == description.media_type
        assert isinstance(description.content, str)
        assert description.content.startswith("John McCain's deeply moving memoir")

        # The cover image simulates the current state of the B&T cover
        # service, where we get a thumbnail-sized image URL in the
        # Axis 360 API response and we can hack the URL to get the
        # full-sized image URL.
        assert LinkRelations.IMAGE == cover.rel
        assert (
            "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Large/Empty"
            == cover.href
        )
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.media_type

        assert LinkRelations.THUMBNAIL_IMAGE == cover.thumbnail.rel
        assert (
            "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Medium/Empty"
            == cover.thumbnail.href
        )
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.thumbnail.media_type

        # Book #1 has a primary author, another author and a narrator.
        #
        # TODO: The narrator data is simulated. we haven't actually
        # verified that Axis 360 sends narrator information in the
        # same format as author information.
        [cont1, cont2, narrator] = bib1.contributors
        assert "McCain, John" == cont1.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == cont1.roles

        assert "Salter, Mark" == cont2.sort_name
        assert (Contributor.Role.AUTHOR,) == cont2.roles

        assert "McCain, John S. III" == narrator.sort_name
        assert (Contributor.Role.NARRATOR,) == narrator.roles

        # Book #2 only has a primary author.
        [cont] = bib2.contributors
        assert "Pollero, Rhonda" == cont.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == cont.roles

        axis_id, isbn = sorted(bib1.identifiers, key=lambda x: x.identifier)
        assert "0003642860" == axis_id.identifier
        assert "9780375504587" == isbn.identifier

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2.subjects, key=lambda x: x.identifier or "")
        assert [
            Subject.BISAC,
            Subject.BISAC,
            Subject.BISAC,
            Subject.AXIS_360_AUDIENCE,
        ] == [x.type for x in subjects]
        general_fiction, women_sleuths, romantic_suspense = sorted(
            x.name for x in subjects if x.type == Subject.BISAC and x.name is not None
        )
        assert "FICTION / General" == general_fiction
        assert "FICTION / Mystery & Detective / Women Sleuths" == women_sleuths
        assert "FICTION / Romance / Suspense" == romantic_suspense

        [adult] = [
            x.identifier for x in subjects if x.type == Subject.AXIS_360_AUDIENCE
        ]
        assert "General Adult" == adult

        # The second book has a cover image simulating some possible
        # future case, where B&T change their cover service so that
        # the size URL hack no longer works. In this case, we treat
        # the image URL as both the full-sized image and the
        # thumbnail.
        [cover] = bib2.links
        assert LinkRelations.IMAGE == cover.rel
        assert "http://some-other-server/image.jpg" == cover.href
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.media_type

        assert LinkRelations.THUMBNAIL_IMAGE == cover.thumbnail.rel
        assert "http://some-other-server/image.jpg" == cover.thumbnail.href
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.thumbnail.media_type

        # The first book is available in two formats -- "ePub" and "AxisNow"
        [adobe, axisnow] = bib1.circulation.formats
        assert Representation.EPUB_MEDIA_TYPE == adobe.content_type
        assert DeliveryMechanism.ADOBE_DRM == adobe.drm_scheme

        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

        # The second book is available in 'Blio' format, which
        # is treated as an alternate name for 'AxisNow'
        [axisnow] = bib2.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_audiobook(self, axis_files_fixture: AxisFilesFixture):
        # TODO - we need a real example to test from. The example we were
        # given is a hacked-up ebook. Ideally we would be able to check
        # narrator information here.
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        assert "Back Spin" == bib.title
        assert Edition.AUDIO_MEDIUM == bib.medium

        # The audiobook has one DeliveryMechanism, in which the Findaway licensing document
        # acts as both the content type and the DRM scheme.
        [findaway] = bib.circulation.formats
        assert None == findaway.content_type
        assert DeliveryMechanism.FINDAWAY_DRM == findaway.drm_scheme

        # Although the audiobook is also available in the "AxisNow"
        # format, no second delivery mechanism was created for it, the
        # way it would have been for an ebook.
        assert b"<formatName>AxisNow</formatName>" in data

    def test_bibliographic_parser_blio_format(
        self, axis_files_fixture: AxisFilesFixture
    ):
        # This book is available as 'Blio' but not 'AxisNow'.
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        data = data.replace(b"Acoustik", b"Blio")
        data = data.replace(b"AxisNow", b"No Such Format")

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        # A book in Blio format is treated as an AxisNow ebook.
        assert Edition.BOOK_MEDIUM == bib.medium
        [axisnow] = bib.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_blio_and_axisnow_format(
        self, axis_files_fixture: AxisFilesFixture
    ):
        # This book is available as both 'Blio' and 'AxisNow'.
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        data = data.replace(b"Acoustik", b"Blio")

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        # There is only one FormatData -- 'Blio' and 'AxisNow' mean the same thing.
        assert Edition.BOOK_MEDIUM == bib.medium
        [axisnow] = bib.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_unsupported_format(
        self, axis_files_fixture: AxisFilesFixture
    ):
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        data = data.replace(b"Acoustik", b"No Such Format 1")
        data = data.replace(b"AxisNow", b"No Such Format 2")

        [[bib, av]] = BibliographicParser().process_all(data)
        assert av is not None
        assert bib is not None

        # We don't support any of the formats, so no FormatData objects were created.
        assert [] == bib.circulation.formats

    def test_parse_author_role(self, axis_files_fixture: AxisFilesFixture):
        """Suffixes on author names are turned into roles."""
        author = "Dyssegaard, Elisabeth Kallick (TRN)"
        parse = BibliographicParser.parse_contributor
        c = parse(author)
        assert "Dyssegaard, Elisabeth Kallick" == c.sort_name
        assert (Contributor.Role.TRANSLATOR,) == c.roles

        # A corporate author is given a normal author role.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False)
        assert "Bob, Inc." == c.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == c.roles

        c = parse(author, primary_author_found=True)
        assert "Bob, Inc." == c.sort_name
        assert (Contributor.Role.AUTHOR,) == c.roles

        # An unknown author type is given an unknown role
        author = "Eve, Mallory (ZZZ)"
        c = parse(author, primary_author_found=False)
        assert "Eve, Mallory" == c.sort_name
        assert (Contributor.Role.UNKNOWN,) == c.roles

        # force_role overwrites whatever other role might be
        # assigned.
        author = "Bob, Inc. (COR)"
        c = parse(
            author, primary_author_found=False, force_role=Contributor.Role.NARRATOR
        )
        assert (Contributor.Role.NARRATOR,) == c.roles

    def test_availability_parser(
        self, axis_files_fixture: AxisFilesFixture, db: DatabaseTransactionFixture
    ):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = axis_files_fixture.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser().process_all(data)

        # We already tested the bibliographic information, so we just make sure
        # it is present.
        assert bib1 is not None
        assert bib2 is not None

        # But we did get availability information.
        assert av1 is not None
        assert av2 is not None

        assert "0003642860" == av1.load_primary_identifier(db.session).identifier
        assert 9 == av1.licenses_owned
        assert 9 == av1.licenses_available
        assert 0 == av1.patrons_in_hold_queue


class TestRaiseExceptionOnError:
    def test_internal_server_error(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("internal_server_error.xml")
        parser = HoldReleaseResponseParser()
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.process_first(data)
        assert "Internal Server Error" in str(excinfo.value)

    def test_ignore_error_codes(self, axis_files_fixture: AxisFilesFixture):
        # A parser subclass can decide not to raise exceptions
        # when encountering specific error codes.
        data = axis_files_fixture.sample_data("internal_server_error.xml")
        retval = object()

        class IgnoreISE(HoldReleaseResponseParser):
            def process_one(self, e, namespaces):
                self.raise_exception_on_error(e, namespaces, ignore_error_codes=[5000])
                return retval

        # Unlike in test_internal_server_error, no exception is
        # raised, because we told the parser to ignore this particular
        # error code.
        parser = IgnoreISE()
        assert retval == parser.process_first(data)

    def test_internal_server_error2(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("invalid_error_code.xml")
        parser = HoldReleaseResponseParser()
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.process_first(data)
        assert "Invalid response code from Axis 360: abcd" in str(excinfo.value)

    def test_missing_error_code(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("missing_error_code.xml")
        parser = HoldReleaseResponseParser()
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.process_first(data)
        assert "No status code!" in str(excinfo.value)


class TestCheckinResponseParser:
    def test_parse_checkin_success(self, axis_files_fixture: AxisFilesFixture):
        # The response parser raises an exception if there's a problem,
        # and returne True otherwise.
        #
        # "Book is not on loan" is not treated as a problem.
        for filename in ("checkin_success.xml", "checkin_not_checked_out.xml"):
            data = axis_files_fixture.sample_data(filename)
            parser = CheckinResponseParser()
            parsed = parser.process_first(data)
            assert parsed is True

    def test_parse_checkin_failure(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("checkin_failure.xml")
        parser = CheckinResponseParser()
        pytest.raises(NotFoundOnRemote, parser.process_first, data)


class TestCheckoutResponseParser:
    def test_parse_checkout_success(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("checkout_success.xml")
        parser = CheckoutResponseParser()
        parsed = parser.process_first(data)
        assert datetime_utc(2015, 8, 11, 18, 57, 42) == parsed

    def test_parse_already_checked_out(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("already_checked_out.xml")
        parser = CheckoutResponseParser()
        pytest.raises(AlreadyCheckedOut, parser.process_first, data)

    def test_parse_not_found_on_remote(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("not_found_on_remote.xml")
        parser = CheckoutResponseParser()
        pytest.raises(NotFoundOnRemote, parser.process_first, data)


class TestHoldResponseParser:
    def test_parse_hold_success(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("place_hold_success.xml")
        parser = HoldResponseParser()
        parsed = parser.process_first(data)
        assert 1 == parsed

    def test_parse_already_on_hold(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("already_on_hold.xml")
        parser = HoldResponseParser()
        pytest.raises(AlreadyOnHold, parser.process_first, data)


class TestHoldReleaseResponseParser:
    def test_success(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("release_hold_success.xml")
        parser = HoldReleaseResponseParser()
        assert True == parser.process_first(data)

    def test_failure(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("release_hold_failure.xml")
        parser = HoldReleaseResponseParser()
        pytest.raises(NotOnHold, parser.process_first, data)


class TestAvailabilityResponseParser:
    """Unlike other response parser tests, this one needs
    access to a real database session, because it needs a real Collection
    to put into its MockAxis360API.
    """

    def test_parse_loan_and_hold(self, axis360: Axis360Fixture):
        data = axis360.sample_text("availability_with_loan_and_hold.xml")
        parser = AvailabilityResponseParser(axis360.api)
        activity = list(parser.process_all(data))
        hold, loan, reserved = sorted(
            activity, key=lambda x: "" if x is None else str(x.identifier)
        )
        assert isinstance(hold, HoldInfo)
        assert isinstance(loan, LoanInfo)
        assert isinstance(reserved, HoldInfo)
        assert axis360.api.collection is not None
        assert axis360.api.collection.id == hold.collection_id
        assert Identifier.AXIS_360_ID == hold.identifier_type
        assert "0012533119" == hold.identifier
        assert 1 == hold.hold_position
        assert hold.end_date is None

        assert axis360.api.collection.id == loan.collection_id
        assert "0015176429" == loan.identifier
        assert isinstance(loan.fulfillment, UrlFulfillment)
        assert "http://fulfillment/" == loan.fulfillment.content_link
        assert datetime_utc(2015, 8, 12, 17, 40, 27) == loan.end_date

        assert axis360.api.collection.id == reserved.collection_id
        assert "1111111111" == reserved.identifier
        assert datetime_utc(2015, 1, 1, 13, 11, 11) == reserved.end_date
        assert 0 == reserved.hold_position

    def test_parse_loan_no_availability(self, axis360: Axis360Fixture):
        data = axis360.sample_text("availability_without_fulfillment.xml")
        parser = AvailabilityResponseParser(axis360.api)
        [loan] = list(parser.process_all(data))
        assert isinstance(loan, LoanInfo)

        assert axis360.api.collection is not None
        assert axis360.api.collection.id == loan.collection_id
        assert "0015176429" == loan.identifier
        assert None == loan.fulfillment
        assert datetime_utc(2015, 8, 12, 17, 40, 27) == loan.end_date

    def test_parse_audiobook_availability(self, axis360: Axis360Fixture):
        data = axis360.sample_text("availability_with_audiobook_fulfillment.xml")
        parser = AvailabilityResponseParser(axis360.api)
        [loan] = list(parser.process_all(data))
        assert isinstance(loan, LoanInfo)
        fulfillment = loan.fulfillment
        assert isinstance(fulfillment, Axis360Fulfillment)

        # The transaction ID is stored as the .key. If we actually
        # need to make a manifest for this book, the key will be used
        # in two more API requests. (See TestAxis360Fulfillment
        # for that.)
        assert "C3F71F8D-1883-2B34-061F-96570678AEB0" == fulfillment.key

        # The API object is present in the Fulfillment and ready to go.
        assert axis360.api == fulfillment.api

    def test_parse_ebook_availability(self, axis360: Axis360Fixture):
        # AvailabilityResponseParser will behave differently depending on whether
        # we ask for the book as an ePub or through AxisNow.
        data = axis360.sample_text("availability_with_ebook_fulfillment.xml")

        # First, ask for an ePub.
        epub_parser = AvailabilityResponseParser(axis360.api, "ePub")
        [availability] = list(epub_parser.process_all(data))
        assert isinstance(availability, LoanInfo)
        fulfillment = availability.fulfillment

        # This particular file has a downloadUrl ready to go, so we
        # get a standard Axis360AcsFulfillment object with that downloadUrl
        # as its content_link.
        assert isinstance(fulfillment, Axis360AcsFulfillment)
        assert not isinstance(fulfillment, Axis360Fulfillment)
        assert (
            "http://adobe.acsm/?src=library&transactionId=2a34598b-12af-41e4-a926-af5e42da7fe5&isbn=9780763654573&format=F2"
            == fulfillment.content_link
        )

        # Next ask for AxisNow -- this will be more like
        # test_parse_audiobook_availability, since it requires an
        # additional API request.

        axisnow_parser = AvailabilityResponseParser(axis360.api, axis360.api.AXISNOW)
        [availability] = list(axisnow_parser.process_all(data))
        assert isinstance(availability, LoanInfo)
        fulfillment = availability.fulfillment
        assert isinstance(fulfillment, Axis360Fulfillment)
        assert "6670197A-D264-447A-86C7-E4CB829C0236" == fulfillment.key

        # The API object is present in the Fulfillment and ready to go
        # make that extra request.
        assert axis360.api == fulfillment.api

    def test_patron_not_found(self, axis360: Axis360Fixture):
        # If the patron is not found, the parser will return an empty list, since
        # that patron can't have any loans or holds.
        data = axis360.sample_text("availability_patron_not_found.xml")
        parser = AvailabilityResponseParser(axis360.api)
        assert list(parser.process_all(data)) == []


class TestJSONResponseParser:
    def test__required_key(self):
        m = JSONResponseParser._required_key
        parsed = dict(key="value")

        # If the value is present, _required_key acts just like get().
        assert "value" == m("key", parsed)

        # If not, it raises a RemoteInitiatedServerError.
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m("absent", parsed)
        assert (
            "Required key absent not present in Axis 360 fulfillment document: {'key': 'value'}"
            in str(excinfo.value)
        )

    def test_verify_status_code(self):
        success = dict(Status=dict(Code=0000))
        failure = dict(Status=dict(Code=1000, Message="A message"))
        missing = dict()

        m = JSONResponseParser.verify_status_code

        # If the document's Status object indicates success, nothing
        # happens.
        m(success)

        # If it indicates failure, an appropriate exception is raised.
        with pytest.raises(PatronAuthorizationFailedException) as excinfo:
            m(failure)
        assert "A message" in str(excinfo.value)

        # If the Status object is missing, a more generic exception is
        # raised.
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(missing)
        assert (
            "Required key Status not present in Axis 360 fulfillment document"
            in str(excinfo.value)
        )

    def test_parse(self):
        class Mock(JSONResponseParser):
            def _parse(self, parsed, **kwargs):
                self.called_with = parsed, kwargs
                return "success"

        parser = Mock()

        # Test success.
        doc = dict(Status=dict(Code=0000))

        # The JSON will be parsed and passed in to _parse(); all other
        # keyword arguments to parse() will be passed through to _parse().
        result = parser.parse(json.dumps(doc), arg2="value2")
        assert "success" == result
        assert (doc, dict(arg2="value2")) == parser.called_with

        # It also works if the JSON was already parsed.
        result = parser.parse(doc, foo="bar")
        assert (doc, {"foo": "bar"}) == parser.called_with

        # Non-JSON input causes an error.
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            parser.parse("I'm not JSON")
        assert (
            'Invalid response from Axis 360 (was expecting JSON): "I\'m not JSON"'
            in str(excinfo.value)
        )


class TestAxis360FulfillmentInfoResponseParser:
    def test__parse_findaway(
        self, axis360: Axis360Fixture, db: DatabaseTransactionFixture
    ) -> None:
        # _parse will create a valid FindawayManifest given a
        # complete document.

        parser = Axis360FulfillmentInfoResponseParser(api=axis360.api)
        m = parser._parse

        edition, pool = db.edition(with_license_pool=True)

        def get_data():
            # We'll be modifying this document to simulate failures,
            # so make it easy to load a fresh copy.
            return json.loads(axis360.sample_data("audiobook_fulfillment_info.json"))

        # This is the data we just got from a call to Axis 360's
        # getfulfillmentInfo endpoint.
        data = get_data()

        # When we call _parse, the API is going to fire off an
        # additional request to the getaudiobookmetadata endpoint, so
        # it can create a complete FindawayManifest. Queue up the
        # response to that request.
        audiobook_metadata = axis360.sample_data("audiobook_metadata.json")
        axis360.api.queue_response(200, {}, audiobook_metadata)

        manifest, expires = m(data, license_pool=pool)

        assert isinstance(manifest, FindawayManifest)
        metadata = manifest.metadata

        # The manifest contains information from the LicensePool's presentation
        # edition
        assert edition.title == metadata["title"]

        # It contains DRM licensing information from Findaway via the
        # Axis 360 API.
        encrypted = metadata["encrypted"]
        assert (
            "0f547af1-38c1-4b1c-8a1a-169d353065d0" == encrypted["findaway:sessionKey"]
        )
        assert "5babb89b16a4ed7d8238f498" == encrypted["findaway:checkoutId"]
        assert "04960" == encrypted["findaway:fulfillmentId"]
        assert "58ee81c6d3d8eb3b05597cdc" == encrypted["findaway:licenseId"]

        # The spine items and duration have been filled in by the call to
        # the getaudiobookmetadata endpoint.
        assert 8150.87 == metadata["duration"]
        assert 5 == len(manifest.readingOrder)

        # We also know when the licensing document expires.
        assert datetime_utc(2018, 9, 29, 18, 34) == expires

        # Now strategically remove required information from the
        # document and verify that extraction fails.
        #
        for field in (
            "FNDContentID",
            "FNDLicenseID",
            "FNDSessionKey",
            "ExpirationDate",
        ):
            missing_field = get_data()
            del missing_field[field]
            with pytest.raises(RemoteInitiatedServerError) as excinfo:
                m(missing_field, license_pool=pool)
            assert "Required key %s not present" % field in str(excinfo.value)

        # Try with a bad expiration date.
        bad_date = get_data()
        bad_date["ExpirationDate"] = "not-a-date"
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(bad_date, license_pool=pool)
        assert "Could not parse expiration date: not-a-date" in str(excinfo.value)

        # Try with an expired session key.
        expired_session_key = get_data()
        expired_session_key["FNDSessionKey"] = "Expired"
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(expired_session_key, license_pool=pool)
        assert "Expired findaway session key" in str(excinfo.value)

    def test__parse_axisnow(
        self, axis360: Axis360Fixture, db: DatabaseTransactionFixture
    ) -> None:
        # _parse will create a valid AxisNowManifest given a
        # complete document.

        parser = Axis360FulfillmentInfoResponseParser(api=axis360.api)
        m = parser._parse

        edition, pool = db.edition(with_license_pool=True)

        def get_data():
            # We'll be modifying this document to simulate failures,
            # so make it easy to load a fresh copy.
            return json.loads(axis360.sample_data("ebook_fulfillment_info.json"))

        # This is the data we just got from a call to Axis 360's
        # getfulfillmentInfo endpoint.
        data = get_data()

        # Since this is an ebook, not an audiobook, there will be no
        # second request to the API, the way there is in the audiobook
        # test.
        manifest, expires = m(data, license_pool=pool)

        assert isinstance(manifest, AxisNowManifest)
        assert {
            "book_vault_uuid": "1c11c31f-81c2-41bb-9179-491114c3f121",
            "isbn": "9780547351551",
        } == json.loads(str(manifest))

        # Try with a bad expiration date.
        bad_date = get_data()
        bad_date["ExpirationDate"] = "not-a-date"
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            m(bad_date, license_pool=pool)
        assert "Could not parse expiration date: not-a-date" in str(excinfo.value)


class TestAudiobookMetadataParser:
    def test__parse(self, axis360: Axis360Fixture):
        # _parse will find the Findaway account ID and
        # the spine items.
        class Mock(AudiobookMetadataParser):
            @classmethod
            def _extract_spine_item(cls, part):
                return part + " (extracted)"

        metadata = dict(
            fndaccountid="An account ID", readingOrder=["Spine item 1", "Spine item 2"]
        )
        account_id, spine_items = Mock()._parse(metadata)

        assert "An account ID" == account_id
        assert ["Spine item 1 (extracted)", "Spine item 2 (extracted)"] == spine_items

        # No data? Nothing will be parsed.
        account_id, spine_items = Mock()._parse({})
        assert None == account_id
        assert [] == spine_items

    def test__extract_spine_item(self, axis360: Axis360Fixture):
        # _extract_spine_item will turn data from Findaway into
        # a SpineItem object.
        m = AudiobookMetadataParser._extract_spine_item
        item = m(
            dict(duration=100.4, fndpart=2, fndsequence=3, title="The Gathering Storm")
        )
        assert isinstance(item, SpineItem)
        assert "The Gathering Storm" == item.title
        assert 2 == item.part
        assert 3 == item.sequence
        assert 100.4 == item.duration
        assert Representation.MP3_MEDIA_TYPE == item.media_type

        # We get a SpineItem even if all the data about the spine item
        # is missing -- these are the default values.
        item = m({})
        assert None == item.title
        assert 0 == item.part
        assert 0 == item.sequence
        assert 0 == item.duration
        assert Representation.MP3_MEDIA_TYPE == item.media_type
