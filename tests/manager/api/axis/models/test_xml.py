import datetime
from contextlib import nullcontext
from typing import Literal

import pytest

from palace.manager.api.axis.models.xml import (
    AddHoldResponse,
    AvailabilityResponse,
    Checkout,
    CheckoutResponse,
    EarlyCheckinResponse,
    Hold,
    RemoveHoldResponse,
)
from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    NotFoundOnRemote,
    PatronAuthorizationFailedException,
)
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.files import AxisFilesFixture


class TestAvailabilityResponse:
    def test_availability_with_loan_and_hold(
        self, axis_files_fixture: AxisFilesFixture
    ):
        data = axis_files_fixture.sample_data("availability_with_loan_and_hold.xml")
        parsed = AvailabilityResponse.from_xml(data)

        [hold, loan, reserved] = parsed.titles

        assert hold.title_id == "0012533119"
        assert hold.availability.is_in_hold_queue
        assert not hold.availability.is_reserved
        assert not hold.availability.is_checked_out
        assert hold.availability.holds_queue_size == 1
        assert hold.availability.holds_queue_position == 1
        assert hold.availability.reserved_end_date is None
        assert hold.availability.available_formats == ["ePub", "Blio"]

        assert loan.title_id == "0015176429"
        assert not loan.availability.is_in_hold_queue
        assert not loan.availability.is_reserved
        assert loan.availability.is_checked_out
        assert loan.availability.checkout_format == "ePub"
        assert loan.availability.download_url == "http://fulfillment/"
        assert loan.availability.checkout_start_date == datetime_utc(
            2015, 7, 22, 17, 40, 27
        )
        assert loan.availability.checkout_end_date == datetime_utc(
            2015, 8, 12, 17, 40, 27
        )
        assert loan.availability.available_formats == ["ePub", "Blio"]

        assert reserved.title_id == "1111111111"
        assert not reserved.availability.is_in_hold_queue
        assert reserved.availability.is_reserved
        assert not reserved.availability.is_checked_out
        assert reserved.availability.reserved_end_date == datetime_utc(
            2015, 1, 1, 13, 11, 11
        )
        assert reserved.availability.holds_queue_position == 1
        assert reserved.availability.available_formats == []

    def test_availability_without_fulfillment(
        self, axis_files_fixture: AxisFilesFixture
    ):
        data = axis_files_fixture.sample_data("availability_without_fulfillment.xml")
        parsed = AvailabilityResponse.from_xml(data)
        [loan] = parsed.titles

        assert loan.title_id == "0015176429"
        assert loan.availability.is_checked_out
        assert loan.availability.checkout_end_date == datetime_utc(
            2015, 8, 12, 17, 40, 27
        )

    def test_availability_with_audiobook_fulfillment(
        self, axis_files_fixture: AxisFilesFixture
    ):
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        parsed = AvailabilityResponse.from_xml(data)
        [loan] = parsed.titles

        assert loan.title_id == "0012244222"
        assert loan.product_title == "Back Spin"
        assert (
            loan.availability.transaction_id == "C3F71F8D-1883-2B34-061F-96570678AEB0"
        )
        assert loan.availability.checkout_format == "Acoustik"

    def test_availability_with_ebook_fulfillment(
        self, axis_files_fixture: AxisFilesFixture
    ):
        # AvailabilityResponseParser will behave differently depending on whether
        # we ask for the book as an ePub or through AxisNow.
        data = axis_files_fixture.sample_data("availability_with_ebook_fulfillment.xml")
        parsed = AvailabilityResponse.from_xml(data)
        [loan] = parsed.titles

        assert loan.title_id == "0016820953"
        assert (
            loan.availability.transaction_id == "6670197A-D264-447A-86C7-E4CB829C0236"
        )
        assert loan.availability.checkout_format == "ePub"
        assert (
            loan.availability.download_url
            == "http://adobe.acsm/?src=library&transactionId=2a34598b-12af-41e4-a926-af5e42da7fe5&isbn=9780763654573&format=F2"
        )

    def test_availability_errors(self, axis_files_fixture: AxisFilesFixture):
        # If the patron is not found, the parser will return an empty list, since
        # that patron can't have any loans or holds.
        data = axis_files_fixture.sample_data("availability_patron_not_found.xml")
        parsed = AvailabilityResponse.from_xml(data)
        assert parsed.titles == []
        assert parsed.status.code == 3122
        assert parsed.status.message == "Patron information is not found."
        parsed.raise_on_error()

        data = axis_files_fixture.sample_data("availability_invalid_token.xml")
        parsed = AvailabilityResponse.from_xml(data)
        assert parsed.titles == []
        assert parsed.status.code == 1001
        assert parsed.status.message == "Authorization token is invalid"
        with pytest.raises(
            PatronAuthorizationFailedException, match="Authorization token is invalid"
        ):
            parsed.raise_on_error()

        data = axis_files_fixture.sample_data("availability_expired_token.xml")
        parsed = AvailabilityResponse.from_xml(data)
        assert parsed.titles == []
        assert parsed.status.code == 1002
        assert parsed.status.message == "Authorization token is expired"
        with pytest.raises(
            PatronAuthorizationFailedException, match="Authorization token is expired"
        ):
            parsed.raise_on_error()

    def test_tiny_collection(self, axis_files_fixture: AxisFilesFixture):
        # Make sure the bibliographic information gets properly
        # collated in preparation for creating Edition objects.

        data = axis_files_fixture.sample_data("tiny_collection.xml")

        [title1, title2] = AvailabilityResponse.from_xml(data).titles

        assert title1.product_title == "Faith of My Fathers : A Family Memoir"
        assert title1.language == "ENGLISH"
        assert title1.publication_date == datetime.date(2000, 3, 7)
        assert title1.publisher == "Random House Inc"
        assert title1.imprint == "Random House Inc"
        assert title1.series is None
        assert title1.annotation.startswith("John McCain's deeply moving memoir")
        assert (
            title1.image_url
            == "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Medium/Empty"
        )
        assert title1.narrators == ["McCain, John S. III"]
        assert title1.contributors == ["McCain, John", "Salter, Mark"]
        assert title1.isbn == "9780375504587"
        assert title1.title_id == "0003642860"
        assert title1.subjects == [
            "BIOGRAPHY & AUTOBIOGRAPHY / Political",
            "BIOGRAPHY & AUTOBIOGRAPHY / Military",
        ]
        assert title1.audience is None
        assert title1.availability.available_formats == ["ePub", "AxisNow"]

        assert title2.product_title == "Slightly Irregular"
        assert title2.language == "ENGLISH"
        assert title2.publication_date == datetime.date(2012, 4, 17)
        assert title2.publisher == "Simon & Schuster"
        assert title2.imprint == "Pocket Books"
        assert title2.series is None
        assert title2.annotation is None
        assert title2.image_url == "http://some-other-server/image.jpg"
        assert title2.narrators == []
        assert title2.contributors == ["Pollero, Rhonda"]
        assert title2.isbn == "9781439100998"
        assert title2.title_id == "0012164897"
        assert title2.subjects == [
            "FICTION / Romance / Suspense",
            "FICTION / Mystery & Detective / Women Sleuths",
            "FICTION / General",
        ]
        assert title2.audience == "General Adult"
        assert title2.availability.available_formats == ["Blio"]

    def test_availability_with_checkouts_and_holds(
        self, axis_files_fixture: AxisFilesFixture
    ):
        # Test that the parser can handle a response with multiple checkouts and holds.
        data = axis_files_fixture.sample_data(
            "availability_with_checkouts_and_holds.xml"
        )
        parsed = AvailabilityResponse.from_xml(data)

        assert len(parsed.titles) == 2

        [title1, title2] = parsed.titles

        assert title1.title_id == "0027094423"
        assert title1.publisher == "Pottermore"
        assert title1.runtime == 77760
        assert title1.availability.available_formats == ["Acoustik"]
        assert title1.availability.available_copies == 2
        assert title1.availability.total_copies == 9
        assert title1.availability.holds_queue_size == 0
        assert title1.availability.checkouts == [
            Checkout(
                patron="01",
                start_date=datetime_utc(2025, 5, 27, 18, 33, 7),
                end_date=datetime_utc(2025, 6, 10, 18, 33),
                format="Acoustik",
                active=True,
            ),
            Checkout(
                patron="02",
                start_date=datetime_utc(2025, 5, 30, 16, 19, 15),
                end_date=datetime_utc(2025, 6, 13, 16, 19),
                format="Acoustik",
                active=True,
            ),
            Checkout(
                patron="03",
                start_date=datetime_utc(2025, 6, 3, 5, 32, 22),
                end_date=datetime_utc(2025, 6, 17, 5, 32),
                format="Acoustik",
                active=True,
            ),
            Checkout(
                patron="04",
                start_date=datetime_utc(2025, 6, 3, 16, 58, 14),
                end_date=datetime_utc(2025, 6, 17, 16, 58),
                format="Acoustik",
                active=True,
            ),
            Checkout(
                patron="05",
                start_date=datetime_utc(2025, 6, 4, 2, 1, 3),
                end_date=datetime_utc(2025, 6, 18, 2, 1),
                format="Acoustik",
                active=True,
            ),
            Checkout(
                patron="06",
                start_date=datetime_utc(2025, 6, 9, 16, 14, 28),
                end_date=datetime_utc(2025, 6, 23, 16, 14),
                format="Acoustik",
                active=True,
            ),
        ]
        assert title1.availability.holds == [
            Hold(
                patron="07",
                email=None,
                hold_date=datetime_utc(2025, 6, 8, 19, 6, 43),
                reserved=True,
            )
        ]

        assert title2.title_id == "0026562458"
        assert title2.product_title == "A Royal Spring"
        assert title2.availability.available_formats == ["Blio", "ePub", "AxisNow"]
        assert title2.availability.available_copies == 0
        assert title2.availability.total_copies == 3
        assert title2.availability.checkouts == []
        assert title2.availability.holds == [
            Hold(
                patron="01",
                email=None,
                hold_date=datetime_utc(2025, 6, 5, 15, 23, 24),
                reserved=True,
            ),
            Hold(
                patron="02",
                email="test@test.com",
                hold_date=datetime_utc(2025, 6, 6, 2, 52, 44),
                reserved=True,
            ),
            Hold(
                patron="03",
                email=None,
                hold_date=datetime_utc(2025, 6, 6, 16, 44, 20),
                reserved=True,
            ),
        ]


class TestEarlyCheckinResponse:
    @pytest.mark.parametrize(
        "filename, exception, code",
        [
            ("checkin_success.xml", False, 0),
            ("checkin_not_checked_out.xml", False, 4058),
            ("checkin_failure.xml", NotFoundOnRemote, 3103),
        ],
    )
    def test_parse(
        self,
        axis_files_fixture: AxisFilesFixture,
        filename: str,
        exception: Literal[False] | type[Exception],
        code: int,
    ) -> None:
        data = axis_files_fixture.sample_data(filename)

        context_manager = (
            nullcontext() if exception is False else pytest.raises(exception)
        )

        with context_manager:
            parsed = EarlyCheckinResponse.from_xml(data)
            assert parsed.status.code == code
            parsed.raise_on_error()


class TestCheckoutResponse:
    def test_parse_checkout_success(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("checkout_success.xml")
        parsed = CheckoutResponse.from_xml(data)
        parsed.raise_on_error()
        assert parsed.status.code == 0
        assert parsed.status.message == "Title Checked out Successfully."
        assert parsed.expiration_date == datetime_utc(2015, 8, 11, 18, 57, 42)

    def test_parse_checkout_success_no_status_message(
        self, axis_files_fixture: AxisFilesFixture
    ):
        data = axis_files_fixture.sample_data("checkout_success_no_status_message.xml")
        parsed = CheckoutResponse.from_xml(data)
        parsed.raise_on_error()
        assert parsed.status.code == 0
        assert parsed.status.message is None
        assert parsed.expiration_date == datetime_utc(2025, 6, 20, 13, 50)

    @pytest.mark.parametrize(
        "filename, exception, code",
        [
            ("already_checked_out.xml", AlreadyCheckedOut, 3110),
            ("not_found_on_remote.xml", NotFoundOnRemote, 3103),
        ],
    )
    def test_parse_checkout_failures(
        self,
        axis_files_fixture: AxisFilesFixture,
        filename: str,
        exception: type[Exception],
        code: int,
    ) -> None:
        data = axis_files_fixture.sample_data(filename)
        parsed = CheckoutResponse.from_xml(data)
        assert parsed.status.code == code
        assert parsed.expiration_date is None

        with pytest.raises(exception):
            parsed.raise_on_error()


class TestAddHoldResponse:
    def test_parse_hold_success(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("place_hold_success.xml")
        parsed = AddHoldResponse.from_xml(data)
        parsed.raise_on_error()
        assert parsed.status.code == 0
        assert parsed.status.message == "Title placed on Hold Successfully."
        assert parsed.holds_queue_position == 1

    def test_parse_hold_already_on_hold(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("already_on_hold.xml")
        parsed = AddHoldResponse.from_xml(data)
        assert parsed.status.code == 3109
        assert parsed.status.message == "Title is in Hold List"
        with pytest.raises(AlreadyOnHold):
            parsed.raise_on_error()


class TestRemoveHoldResponse:
    def test_parse_success(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("release_hold_success.xml")
        parsed = RemoveHoldResponse.from_xml(data)
        assert parsed.status.code == 0
        assert parsed.status.message == "Title removed from Hold Successfully."
        parsed.raise_on_error()

    def test_parse_not_on_hold(self, axis_files_fixture: AxisFilesFixture):
        data = axis_files_fixture.sample_data("release_hold_failure.xml")
        parsed = RemoveHoldResponse.from_xml(data)
        assert parsed.status.code == 3109
        assert parsed.status.message == "This title is not in Hold list."
        parsed.raise_on_error()
