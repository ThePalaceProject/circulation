import pytest

from palace.manager.api.overdrive.model import Checkout, Checkouts, LinkTemplate
from palace.manager.core.exceptions import PalaceValueError
from tests.fixtures.files import OverdriveFilesFixture


def test_link_template() -> None:
    template = LinkTemplate(
        href="http://example.com/{foo}/{bar}", type="application/json"
    )
    assert template.href == "http://example.com/{foo}/{bar}"
    assert template.type == "application/json"
    assert template.substitutions == {"foo", "bar"}
    assert template.template(foo="baz", bar="qux") == "http://example.com/baz/qux"

    # Test templating a string that needs to be URL encoded.
    template = LinkTemplate(href="http://example.com/{foo}", type="application/json")
    assert template.template(foo="baz qux:/") == "http://example.com/baz+qux%3A%2F"

    # A URL with no substitutions
    template = LinkTemplate(href="http://example.com/", type="application/json")
    assert template.substitutions == set()
    assert template.template() == "http://example.com/"

    # Test missing substitution
    template = LinkTemplate(
        href="http://example.com/{foo}/{bar}/{baz}", type="application/json"
    )
    with pytest.raises(PalaceValueError, match="Missing substitutions: bar, foo"):
        template.template(baz="qux")


def test_checkouts(overdrive_files_fixture: OverdriveFilesFixture) -> None:
    checkouts = Checkouts.model_validate_json(
        overdrive_files_fixture.sample_data("no_loans.json")
    )
    assert checkouts.total_items == 0
    assert checkouts.total_checkouts == 0
    assert checkouts.checkouts == []
    assert len(checkouts.links) == 1
    assert (
        checkouts.links["self"].href
        == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/"
    )

    checkouts = Checkouts.model_validate_json(
        overdrive_files_fixture.sample_data("shelf_with_some_checked_out_books.json")
    )
    assert len(checkouts.links) == 1
    assert (
        checkouts.links["self"].href
        == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/"
    )
    assert checkouts.total_items == 4
    assert checkouts.total_checkouts == 4
    assert len(checkouts.checkouts) == 4

    checkouts = Checkouts.model_validate_json(
        overdrive_files_fixture.sample_data(
            "shelf_with_book_already_fulfilled_on_kindle.json"
        )
    )
    assert len(checkouts.links) == 1
    assert (
        checkouts.links["self"].href
        == "http://patron.api.overdrive.com/v1/patrons/me/checkouts"
    )
    assert checkouts.total_items == 2
    assert checkouts.total_checkouts == 2
    assert len(checkouts.checkouts) == 2


def test_checkout(overdrive_files_fixture: OverdriveFilesFixture) -> None:
    checkout = Checkout.model_validate_json(
        overdrive_files_fixture.sample_data(
            "checkout_response_book_fulfilled_on_kindle.json"
        )
    )
    assert checkout.reserve_id == "98EA8135-52C0-4480-9C0E-1D0779670D4A"

    checkout = Checkout.model_validate_json(
        overdrive_files_fixture.sample_data("checkout_response_locked_in_format.json")
    )
    assert checkout.reserve_id == "76C1B7D0-17F4-4C05-8397-C66C17411584"
    assert checkout.locked_in is True
    assert checkout.expires.year == 2013
    assert checkout.expires.month == 10
    assert checkout.expires.day == 4

    assert len(checkout.formats) == 2
    assert checkout.get_format("unknown") is None

    epub_format = checkout.get_format("ebook-epub-adobe")
    assert epub_format is not None
    assert epub_format.format_type == "ebook-epub-adobe"
    assert len(epub_format.links) == 1
    assert (
        epub_format.links["self"].href
        == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe"
    )
    assert len(epub_format.link_templates) == 1

    ebook_format = checkout.get_format("ebook-overdrive")
    assert ebook_format is not None
    assert ebook_format.format_type == "ebook-overdrive"
    assert len(ebook_format.links) == 1
    assert (
        ebook_format.links["self"].href
        == "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-overdrive"
    )

    checkout = Checkout.model_validate_json(
        overdrive_files_fixture.sample_data("single_loan.json")
    )
    assert checkout.reserve_id == "2BF132F7-215E-461B-B103-007CCED1915A"
