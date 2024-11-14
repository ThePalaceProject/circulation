import pytest
from pydantic import ValidationError

from palace.manager.opds import opds2
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


def test_feed_odl_success(
    opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
) -> None:
    """
    Parse and validate a basic OPDS2 + ODL feed.
    """
    Feed.model_validate_json(opds2_with_odl_files_fixture.sample_data("feed2.json"))


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
