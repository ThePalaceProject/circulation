import json
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from palace.manager.opds.opds2 import (
    Availability,
    Publication,
    PublicationFeed,
    PublicationFeedNoValidation,
)
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.files import OPDS2FilesFixture


@pytest.mark.parametrize(
    "filename, publication_identifiers",
    [
        pytest.param(
            "feed.json",
            [
                "urn:isbn:978-3-16-148410-0",
                "http://example.org/huckleberry-finn",
                "urn:proquest.com/document-id/181639",
            ],
            id="feed.json",
        ),
        pytest.param(
            "feed2.json",
            [
                "urn:uuid:5d7d7820-1a47-49d2-b9fc-dcd09935c0ea",
                "urn:uuid:1efd6eb1-4f0b-47d1-922c-7d68c9960596",
                "urn:uuid:c66bd95a-18cd-49be-8e0f-1f2a85a0a94b",
            ],
            id="feed2.json",
        ),
        pytest.param(
            "auth_token_feed.json",
            [
                "urn:proquest.com/document-id/1543720",
                "urn:proquest.com/document-id/6940768",
            ],
            id="auth_token_feed.json",
        ),
    ],
)
def test_publication_feed(
    filename: str,
    publication_identifiers: list[str],
    opds2_files_fixture: OPDS2FilesFixture,
) -> None:
    """
    Do some basic validation that an OPDS2 feed can be parsed correctly.
    """

    feed = PublicationFeed.model_validate_json(
        opds2_files_fixture.sample_data(filename)
    )
    assert len(feed.publications) == len(publication_identifiers)

    for publication, identifier in zip(feed.publications, publication_identifiers):
        assert isinstance(publication, Publication)
        assert publication.metadata.identifier == identifier


def test_publication_feed_tf(
    opds2_files_fixture: OPDS2FilesFixture,
) -> None:
    """Test Taylor and Francis feed parsing, especially ISO 8601 date handling."""
    feed = PublicationFeed.model_validate_json(
        opds2_files_fixture.sample_data("tf.json")
    )
    assert len(feed.publications) == 4

    # Test first publication - full date format
    pub1 = feed.publications[0]
    assert pub1.metadata.identifier == "urn:isbn:9780203992104"
    assert pub1.metadata.title == "The Economic Consequences of the Gulf War"
    assert pub1.metadata.published == datetime(2005, 10, 26, tzinfo=timezone.utc)
    assert pub1.metadata.modified == datetime(
        2025, 9, 23, 16, 46, 28, tzinfo=timezone.utc
    )
    assert pub1.metadata.publisher == "Routledge"
    assert pub1.metadata.author == ("Kamran Mofid",)

    # Test second publication - year-only date
    pub2 = feed.publications[1]
    assert pub2.metadata.identifier == "urn:isbn:9780429198069"
    assert pub2.metadata.title == "Hunger and Famine in the Long Nineteenth Century"
    # Year-only "2022" should parse as 2022-01-01T00:00:00Z, NOT as Unix timestamp
    assert pub2.metadata.published == datetime(2022, 1, 1, tzinfo=timezone.utc)
    assert pub2.metadata.modified == datetime(
        2025, 9, 23, 16, 46, 28, tzinfo=timezone.utc
    )
    assert pub2.metadata.publisher == "Routledge"
    assert pub2.metadata.editor == ("Gail Turley Houston",)

    # Test third publication - full date format
    pub3 = feed.publications[2]
    assert pub3.metadata.identifier == "urn:isbn:9780429169779"
    assert pub3.metadata.title == "Handbook of Surface and Colloid Chemistry"
    assert pub3.metadata.published == datetime(2015, 6, 25, tzinfo=timezone.utc)
    assert pub3.metadata.modified == datetime(
        2025, 9, 23, 16, 46, 28, tzinfo=timezone.utc
    )
    assert pub3.metadata.publisher == "CRC Press"
    assert pub3.metadata.editor == ("K. S. Birdi",)

    # Test fourth publication - full date format
    pub4 = feed.publications[3]
    assert pub4.metadata.identifier == "urn:isbn:9781315629889"
    assert pub4.metadata.title == "Radical Sensibility"
    assert pub4.metadata.published == datetime(2016, 4, 6, tzinfo=timezone.utc)
    assert pub4.metadata.modified == datetime(
        2025, 9, 23, 16, 46, 28, tzinfo=timezone.utc
    )
    assert pub4.metadata.publisher == "Routledge"
    assert pub4.metadata.author == ("Chris Jones",)


def test_publication_feed_no_publications(
    opds2_files_fixture: OPDS2FilesFixture,
) -> None:

    feed_dict = json.loads(opds2_files_fixture.sample_data("feed.json"))

    # Remove the publications key from the feed
    del feed_dict["publications"]

    # This should raise an error because the feed is missing the publications key
    with pytest.raises(ValidationError) as exc_info:
        PublicationFeed.model_validate(feed_dict)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0].get("msg") == "Field required"

    # But an empty list of publications should be fine, this can happen on the
    # last page of a paginated feed
    feed_dict["publications"] = []
    feed = PublicationFeed.model_validate(feed_dict)
    assert isinstance(feed.publications, list)
    assert len(feed.publications) == 0


def test_publication_feed_failures(
    opds2_files_fixture: OPDS2FilesFixture,
) -> None:
    """
    Test that the parser gives errors when the feed fails to parse.
    """
    with pytest.raises(ValidationError) as exc_info:
        PublicationFeed.model_validate_json(
            opds2_files_fixture.sample_data("bad_feed.json")
        )

    errors = exc_info.value.errors()
    assert len(errors) == 10
    assert [e.get("msg") for e in errors] == [
        "Field required",
        "Field required",
        "Value error, Invalid language code 'enzz'",
        "Value error, Invalid language code 'qx'",
        "Input should be a valid string or OPDS object",
        "Field required",
        "Input should be a valid string, OPDS object, or list",
        "Field required",
        "Field required",
        "Field required",
    ]

    with pytest.raises(ValidationError) as exc_info:
        PublicationFeed.model_validate_json(
            opds2_files_fixture.sample_data("bad_feed2.json")
        )

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert [e.get("msg") for e in errors] == [
        "Value error, Invalid language code 'this is a bad language code'",
    ]


def test_publication_feed_no_validation(
    opds2_files_fixture: OPDS2FilesFixture,
) -> None:
    """
    Test that we can parse a bad feed without validation,
    then parse the publications individually to see if they
    are valid.
    """
    feed_parsed = PublicationFeedNoValidation.model_validate_json(
        opds2_files_fixture.sample_data("bad_feed.json")
    )

    assert feed_parsed.metadata.title == "Example listing publications"
    assert (
        feed_parsed.links.get(rel="self", raising=True).href == "http://example.com/new"
    )
    assert len(feed_parsed.publications) == 3

    for publication_dict in feed_parsed.publications:
        with pytest.raises(ValidationError):
            Publication.model_validate(publication_dict)


class TestAvailability:
    """Test the Availability model and its fields."""

    @pytest.mark.parametrize(
        "since_value",
        [
            pytest.param("2023-01-15T10:30:00Z", id="datetime_with_Z"),
            pytest.param("2023-01-15T10:30:00+00:00", id="datetime_with_offset_zero"),
            pytest.param(
                "2023-01-15T10:30:00.123456Z", id="datetime_with_microseconds"
            ),
            pytest.param(
                "2023-12-25T23:59:59.999999+05:30", id="datetime_with_positive_offset"
            ),
            pytest.param(
                "2023-12-25T23:59:59-08:00", id="datetime_with_negative_offset"
            ),
        ],
    )
    def test_since_parsing(self, since_value: str) -> None:
        """Test that the since field is parsed correctly with various datetime formats."""
        availability = Availability.model_validate({"since": since_value})
        assert availability.since is not None
        assert availability.since.tzinfo is not None

    def test_since_optional(self) -> None:
        """Test that the since field is optional."""
        availability = Availability.model_validate({})
        assert availability.since is None

    def test_since_cannot_be_future(self) -> None:
        """Test that the since field cannot be in the future."""
        future_date = (utc_now() + timedelta(days=1)).isoformat()
        with pytest.raises(ValidationError, match="Datetime must be in the past"):
            Availability.model_validate({"since": future_date})

    @pytest.mark.parametrize(
        ("invalid_since", "error_pattern"),
        [
            pytest.param(
                "2023-01-15T10:30:00",
                "Input should have timezone info",
                id="datetime_missing_timezone",
            ),
            pytest.param(
                "not-a-datetime",
                "Input should be a valid datetime",
                id="invalid_string",
            ),
        ],
    )
    def test_since_invalid_formats(
        self, invalid_since: str, error_pattern: str
    ) -> None:
        """Test that invalid datetime formats raise ValidationError."""
        with pytest.raises(ValidationError, match=error_pattern):
            Availability.model_validate({"since": invalid_since})
