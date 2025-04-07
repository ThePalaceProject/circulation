import json

import pytest
from pydantic import ValidationError

from palace.manager.opds.opds2 import (
    Publication,
    PublicationFeed,
    PublicationFeedNoValidation,
)
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
