import pytest
from pydantic import ValidationError

from palace.manager.opds import opds2
from palace.manager.opds.a11y import (
    AccessibilityFeature,
    AccessMode,
    AccessModeSufficient,
)
from palace.manager.opds.odl.odl import Feed, Publication, _get_publication_type
from tests.fixtures.files import OPDS2FilesFixture, OPDS2WithODLFilesFixture


def test__get_publication_type() -> None:
    """
    The discriminator function should return the correct
    publication type based on the input.
    """
    assert _get_publication_type({}) == "Opds2Publication"
    assert _get_publication_type({"licenses": []}) == "OdlPublication"

    assert _get_publication_type(opds2.Publication.model_construct()) == "Opds2Publication"  # type: ignore[call-arg]
    assert _get_publication_type(Publication.model_construct()) == "OdlPublication"  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "filename",
    [
        "feed.json",
        "feed2.json",
        "auth_token_feed.json",
    ],
)
def test_feed_opds(filename: str, opds2_files_fixture: OPDS2FilesFixture) -> None:
    """
    The ODL parser should be able to parse an OPDS2 feed, as
    well as a feed with ODL publications.
    """
    Feed.model_validate_json(opds2_files_fixture.sample_data(filename))


@pytest.mark.parametrize(
    "filename",
    [
        "feed2.json",
        "a11y.json",
    ],
)
def test_feed_odl_success(
    filename: str,
    opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
) -> None:
    """
    Parse and validate a basic OPDS2 + ODL feed.
    """
    Feed.model_validate_json(opds2_with_odl_files_fixture.sample_data(filename))


def test_feed_odl_a11y(
    opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
) -> None:
    """Verify accessibility metadata is parsed correctly from an ODL feed."""
    feed = Feed.model_validate_json(
        opds2_with_odl_files_fixture.sample_data("a11y.json")
    )
    assert len(feed.publications) == 5

    # Publication 0: minimal — single feature only
    pub = feed.publications[0]
    assert pub.metadata.identifier == "urn:ISBN:9781667404769"
    a11y = pub.metadata.accessibility
    assert a11y.feature == [AccessibilityFeature.table_of_contents]
    assert a11y.access_mode == []
    assert a11y.access_mode_sufficient == []
    assert a11y.conforms_to is None
    assert a11y.summary is None

    # Publication 1: audiobook — accessMode only
    pub = feed.publications[1]
    assert pub.metadata.identifier == "urn:ISBN:9781603935517"
    a11y = pub.metadata.accessibility
    assert a11y.access_mode == [AccessMode.auditory]
    assert a11y.feature == []

    # Publication 2: richest metadata — conformsTo, sufficient modes, many features, summary
    pub = feed.publications[2]
    assert pub.metadata.identifier == "urn:ISBN:9781476733531"
    a11y = pub.metadata.accessibility
    assert (
        a11y.conforms_to
        == "http://www.idpf.org/epub/a11y/accessibility-20170105.html#wcag-aa"
    )
    assert a11y.conformance_profiles == (
        "http://www.idpf.org/epub/a11y/accessibility-20170105.html#wcag-aa",
    )
    assert a11y.sufficient_access_modes == ((AccessModeSufficient.textual,),)
    assert AccessibilityFeature.alternative_text in a11y.feature
    assert AccessibilityFeature.book_index in a11y.feature
    assert AccessibilityFeature.long_description in a11y.feature
    assert AccessibilityFeature.print_page_numbers in a11y.feature
    assert a11y.summary is not None
    assert "WCAG-AA" in a11y.summary

    # Publication 3: multiple features, no conformance
    pub = feed.publications[3]
    assert pub.metadata.identifier == "urn:ISBN:9780062096531"
    a11y = pub.metadata.accessibility
    assert a11y.feature == [
        AccessibilityFeature.table_of_contents,
        AccessibilityFeature.print_page_numbers,
        AccessibilityFeature.reading_order,
    ]
    assert a11y.conforms_to is None

    # Publication 4: sufficient modes + summary, no conformsTo
    pub = feed.publications[4]
    assert pub.metadata.identifier == "urn:ISBN:9781524733469"
    a11y = pub.metadata.accessibility
    assert a11y.sufficient_access_modes == ((AccessModeSufficient.textual,),)
    assert a11y.conforms_to is None
    assert a11y.summary is not None
    assert AccessibilityFeature.structural_navigation in a11y.feature


def test_feed_odl_failure(
    opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
) -> None:
    """
    The ODL parser should fail to parse an OPDS2 feed with an ODL publication.
    """
    with pytest.raises(ValidationError) as exc_info:
        Feed.model_validate_json(opds2_with_odl_files_fixture.sample_data("feed.json"))

    errors = exc_info.value.errors()
    assert len(errors) == 2
