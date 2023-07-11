import json
import uuid
from datetime import date, timedelta
from unittest.mock import Mock

import pytest

from api.admin.announcement_list_validator import AnnouncementListValidator
from core.model.announcements import AnnouncementData
from core.problem_details import INVALID_INPUT
from core.util.problem_detail import ProblemDetail, ProblemError
from tests.fixtures.announcements import AnnouncementFixture


class TestAnnouncementListValidator:
    def assert_invalid(self, x, detail):
        assert isinstance(x, ProblemDetail)
        assert INVALID_INPUT.uri == x.uri
        assert detail == x.detail

    def test_defaults(self):
        validator = AnnouncementListValidator()
        assert 3 == validator.maximum_announcements
        assert 15 == validator.minimum_announcement_length
        assert 350 == validator.maximum_announcement_length
        assert 60 == validator.default_duration_days

    def test_validate_announcements(self):
        # validate_announcement succeeds if every individual announcement succeeds,
        # and if some additional checks pass on the announcement list as a whole.

        validator = AnnouncementListValidator(maximum_announcements=2)
        validator.validate_announcement = Mock(
            side_effect=lambda x: AnnouncementData(**x)
        )
        m = validator.validate_announcements

        # validate_announcements calls validate_announcement on every
        # announcement in a list, so this...
        before = [
            {
                "id": "id1",
                "content": "announcement1",
                "start": "2020-01-01",
                "finish": "2020-01-02",
            },
            {
                "id": "id2",
                "content": "announcement2",
                "start": "2020-02-02",
                "finish": "2020-02-03",
            },
        ]

        # ...should become this.
        after = {
            "id1": AnnouncementData(**before[0]),
            "id2": AnnouncementData(**before[1]),
        }
        validated = m(before)
        assert validated == after

        # If a JSON string is passed in, it will be decoded before
        # processing.
        assert m(json.dumps(before)) == after

        # If you pass in something other than a list or JSON-encoded
        # list, you get a ProblemDetail.
        for invalid in dict(), json.dumps(dict()), "non-json string":
            with pytest.raises(ProblemError) as excinfo:
                m(invalid)
            assert isinstance(excinfo.value, ProblemError)
            assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
            assert (
                "Invalid announcement list format"
                in excinfo.value.problem_detail.detail
            )

        # validate_announcements runs some checks on the list of announcements.
        # Each validator has a maximum length it will accept.
        too_many = [
            {
                "id": "announcement1",
                "content": "announcement1",
                "start": "2020-01-01",
                "finish": "2020-01-02",
            },
            {
                "id": "announcement2",
                "content": "announcement1",
                "start": "2020-01-01",
                "finish": "2020-01-02",
            },
            {
                "id": "announcement3",
                "content": "announcement1",
                "start": "2020-01-01",
                "finish": "2020-01-02",
            },
        ]
        with pytest.raises(ProblemError) as excinfo:
            m(too_many)
        assert isinstance(excinfo.value, ProblemError)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Too many announcements: maximum is 2"
            in excinfo.value.problem_detail.detail
        )

        # A list of announcements will be rejected if it contains duplicate IDs.
        duplicate_ids = [
            {
                "id": "announcement1",
                "content": "announcement1",
                "start": "2020-01-01",
                "finish": "2020-01-02",
            },
            {
                "id": "announcement1",
                "content": "announcement1",
                "start": "2020-01-01",
                "finish": "2020-01-02",
            },
        ]
        with pytest.raises(ProblemError) as excinfo:
            m(duplicate_ids)
        assert isinstance(excinfo.value, ProblemError)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Duplicate announcement ID: announcement1"
            in excinfo.value.problem_detail.detail
        )

    def test_validate_announcement_success(
        self, announcement_fixture: AnnouncementFixture
    ):
        # End-to-end test of validate_announcement in successful scenarios.
        validator = AnnouncementListValidator()
        m = validator.validate_announcement

        # Simulate the creation of a new announcement -- no incoming ID.
        today = announcement_fixture.today
        in_a_week = announcement_fixture.in_a_week
        valid = dict(
            start=today.strftime("%Y-%m-%d"),
            finish=in_a_week.strftime("%Y-%m-%d"),
            content="This is a test of announcement validation.",
        )

        validated = m(valid)

        # A UUID has been created for the announcement.
        assert isinstance(validated.id, uuid.UUID)
        id = str(validated.id)
        for position in 8, 13, 18, 23:
            assert "-" == id[position]

        # Date strings have been converted to date objects.
        assert today == validated.start
        assert in_a_week == validated.finish

        # Now simulate an edit, where an ID is provided.
        valid["id"] = str(uuid.uuid4())

        # Now the incoming data is validated and only the id is changed
        validated2 = m(valid)
        assert validated2.id != validated.id
        assert validated2.start == validated.start
        assert validated2.finish == validated.finish
        assert validated2.content == validated.content
        assert str(validated2.id) == valid["id"]

        # If no start date is specified, today's date is used. If no
        # finish date is specified, a default associated with the
        # validator is used.
        no_finish_date = dict(content="This is a test of announcment validation")
        validated = m(no_finish_date)
        assert today == validated.start
        assert (
            today + timedelta(days=validator.default_duration_days) == validated.finish
        )

    def test_validate_announcement_failure(self):
        # End-to-end tests of validation failures for a single
        # announcement.
        validator = AnnouncementListValidator()
        m = validator.validate_announcement

        # Totally bogus format
        for invalid in '{"a": "string"}', ["a list"]:
            with pytest.raises(ProblemError) as excinfo:
                m(invalid)
            assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
            assert "Invalid announcement format" in excinfo.value.problem_detail.detail

        # Some baseline valid value to use in tests where _some_ of the data is valid.
        today = date.today()
        tomorrow = today + timedelta(days=1)
        message = "An important message to all patrons: reading is FUN-damental!"

        # Missing a required field
        no_content = dict(start=today)
        with pytest.raises(ProblemError) as excinfo:
            m(no_content)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert "Missing required field: content" in excinfo.value.problem_detail.detail

        # Bad content -- tested at greater length in another test.
        bad_content = dict(start=today, content="short")
        with pytest.raises(ProblemError) as excinfo:
            m(bad_content)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Value too short (5 versus 15 characters): short"
            in excinfo.value.problem_detail.detail
        )

        # Bad id
        bad_id = dict(id="not-a-uuid", start=today, content=message)
        with pytest.raises(ProblemError) as excinfo:
            m(bad_id)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Invalid announcement ID: not-a-uuid" in excinfo.value.problem_detail.detail
        )

        # Bad start date -- tested at greater length in another test.
        bad_start_date = dict(start="not-a-date", content=message)
        with pytest.raises(ProblemError) as excinfo:
            m(bad_start_date)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Value for start is not a date: not-a-date"
            in excinfo.value.problem_detail.detail
        )

        # Bad finish date.
        yesterday = today - timedelta(days=1)
        for bad_finish_date in (today, yesterday):
            bad_data = dict(start=today, finish=bad_finish_date, content=message)
            with pytest.raises(ProblemError) as excinfo:
                m(bad_data)
            assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
            assert (
                "Value for finish must be no earlier than %s"
                % (tomorrow.strftime(validator.DATE_FORMAT))
                in excinfo.value.problem_detail.detail
            )

    def test_validate_length(self):
        # Test the validate_length helper method in more detail than
        # it's tested in validate_announcement.
        m = AnnouncementListValidator.validate_length
        value = "four"
        assert value == m(value, 3, 5)

        with pytest.raises(ProblemError) as excinfo:
            m(value, 10, 20)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Value too short (4 versus 10 characters): four"
            in excinfo.value.problem_detail.detail
        )

        with pytest.raises(ProblemError) as excinfo:
            m(value, 1, 3)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Value too long (4 versus 3 characters): four"
            in excinfo.value.problem_detail.detail
        )

    def test_validate_date(self):
        # Test the validate_date helper method in more detail than
        # it's tested in validate_announcement.
        m = AnnouncementListValidator.validate_date

        february_1 = date(2020, 2, 1)

        # The incoming date can be either a string or date.
        # The output is always a date.
        assert february_1 == m("somedate", "2020-2-1")
        assert february_1 == m("somedate", february_1)

        # But if a string is used, it must be in a specific format.
        with pytest.raises(ProblemError) as excinfo:
            m("somedate", "not-a-date")
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Value for somedate is not a date: not-a-date"
            in excinfo.value.problem_detail.detail
        )

        # If a minimum (date) is provided, the selection
        # must be on or after that date.

        january_1 = date(2020, 1, 1)
        assert february_1 == m("somedate", february_1, minimum=january_1)

        with pytest.raises(ProblemError) as excinfo:
            m("somedate", january_1, minimum=february_1)
        assert INVALID_INPUT.uri == excinfo.value.problem_detail.uri
        assert (
            "Value for somedate must be no earlier than 2020-02-01"
            in excinfo.value.problem_detail.detail
        )
