import json

import pytest
from pydantic import TypeAdapter

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.opds.types.link import (
    BaseLink,
    CompactCollection,
    validate_unique_links,
)


class TestBaseLink:
    def test_rels(self):
        link = BaseLink(href="http://example.com", rel="foo")
        assert link.rels == ("foo",)
        link = BaseLink(href="http://example.com", rel=("foo", "bar"))
        assert link.rels == ("foo", "bar")

    def test_href_templated(self):
        link = BaseLink(href="http://example.com", rel="foo")
        assert link.href_templated() == "http://example.com"
        link = BaseLink(href="http://example.com/{?x,y,z}", rel="foo", templated=True)
        assert (
            link.href_templated({"x": 1, "y": "foo"}) == "http://example.com/?x=1&y=foo"
        )


class CompactCollectionFixture:
    def __init__(self):
        self.foo_link = BaseLink(
            href="http://example.com/foo", rel="foo", type="application/xyz"
        )
        self.bar_link = BaseLink(href="http://example.com/bar", rel="bar")
        self.baz_link = BaseLink(
            href="http://example.com/baz", rel="bar", type="application/xyz"
        )
        self.bam_link = BaseLink(
            href="http://example.com/bam", rel="bam", type="application/abc"
        )
        self.fizz_link = BaseLink(
            href="http://example.com/fizz", rel="foo", type="application/xyz"
        )

        self.list = [
            self.foo_link,
            self.bar_link,
            self.baz_link,
            self.bam_link,
            self.fizz_link,
        ]
        self.links = CompactCollection(self.list)
        self.validator = TypeAdapter(CompactCollection[BaseLink])


@pytest.fixture
def list_of_links_fixture():
    return CompactCollectionFixture()


class TestCompactCollection:
    def test_boolean(self, list_of_links_fixture: CompactCollectionFixture) -> None:
        assert list_of_links_fixture.links
        assert not CompactCollection([])

    def test_get_collection(
        self, list_of_links_fixture: CompactCollectionFixture
    ) -> None:
        links = list_of_links_fixture.links
        assert links.get_collection() == links
        assert links.get_collection(rel="bar") == CompactCollection(
            (
                list_of_links_fixture.bar_link,
                list_of_links_fixture.baz_link,
            )
        )
        assert links.get_collection(type="application/xyz") == CompactCollection(
            (
                list_of_links_fixture.foo_link,
                list_of_links_fixture.baz_link,
                list_of_links_fixture.fizz_link,
            )
        )
        assert links.get_collection(
            rel="bar", type="application/xyz"
        ) == CompactCollection((list_of_links_fixture.baz_link,))

    def test_get(self, list_of_links_fixture: CompactCollectionFixture) -> None:
        links = list_of_links_fixture.links
        assert links.get() == list_of_links_fixture.foo_link
        assert links.get(rel="foo") == list_of_links_fixture.foo_link
        assert links.get(type="application/xyz") == list_of_links_fixture.foo_link
        assert links.get(type="application/abc") == list_of_links_fixture.bam_link
        assert (
            links.get(rel="bar", type="application/xyz")
            == list_of_links_fixture.baz_link
        )
        assert links.get(rel="nonexistent") is None
        assert links.get(type="nonexistent") is None
        assert links.get(rel="nonexistent", type="nonexistent") is None

        with pytest.raises(
            PalaceValueError, match="^No links found with rel='nonexistent'$"
        ):
            links.get(rel="nonexistent", raising=True)

        with pytest.raises(PalaceValueError, match="^Multiple links found$"):
            links.get(raising=True)
        with pytest.raises(
            PalaceValueError, match="^Multiple links found with type='application/xyz'$"
        ):
            links.get(type="application/xyz", raising=True)
        with pytest.raises(
            PalaceValueError, match="^Multiple links found with rel='bar'$"
        ):
            links.get(rel="bar", raising=True)
        with pytest.raises(
            PalaceValueError,
            match="^Multiple links found with rel='foo' and type='application/xyz'$",
        ):
            links.get(rel="foo", type="application/xyz", raising=True)

    def test_validate(self, list_of_links_fixture: CompactCollectionFixture) -> None:
        validator = list_of_links_fixture.validator

        # The list of links is valid, so it should return the same list.
        validated = validator.validate_python(list_of_links_fixture.list)
        assert validated == list_of_links_fixture.links
        assert isinstance(validated, CompactCollection)
        for link in validated:
            assert isinstance(link, BaseLink)

        # Duplicate links are accepted by CompactCollection itself.
        # Uniqueness is enforced at the field level via validate_unique_links.
        duplicate_list = list_of_links_fixture.list + [list_of_links_fixture.foo_link]
        validated_with_dup = validator.validate_python(duplicate_list)
        assert len(validated_with_dup) == len(duplicate_list)

        # Load the list of links from a JSON object.
        json_obj = json.dumps(
            [
                {"href": "http://example.com/foo", "rel": "foo"},
                {
                    "href": "http://example.com/bar",
                    "rel": "bar",
                    "type": "application/xyz",
                },
            ],
            separators=(",", ":"),
        )
        validated = validator.validate_json(json_obj)
        assert len(validated) == 2
        assert isinstance(validated, CompactCollection)
        for link in validated:
            assert isinstance(link, BaseLink)

        [first, second] = validated
        assert first.href == "http://example.com/foo"
        assert first.rel == "foo"
        assert first.type is None

        assert second.href == "http://example.com/bar"
        assert second.rel == "bar"
        assert second.type == "application/xyz"

        assert validator.dump_json(validated, exclude_unset=True) == json_obj.encode()
        assert validator.dump_python(validated, exclude_unset=True) == validated

    def test_queries_with_shared_rel_type_href(self) -> None:
        """Query methods work correctly when links share (rel, type, href)
        but differ in other fields (e.g., templated)."""
        link_a = BaseLink(href="http://example.com/foo", rel="foo", type="text/html")
        link_b = BaseLink(
            href="http://example.com/foo",
            rel="foo",
            type="text/html",
            templated=True,
        )
        link_c = BaseLink(href="http://example.com/bar", rel="bar")
        links = CompactCollection([link_a, link_b, link_c])

        # get_collection returns both links matching the rel and type.
        by_rel = links.get_collection(rel="foo")
        assert len(by_rel) == 2
        assert by_rel == CompactCollection([link_a, link_b])

        by_type = links.get_collection(type="text/html")
        assert len(by_type) == 2
        assert by_type == CompactCollection([link_a, link_b])

        by_rel_type = links.get_collection(rel="foo", type="text/html")
        assert len(by_rel_type) == 2
        assert by_rel_type == CompactCollection([link_a, link_b])

        # get returns the first match when multiple exist.
        assert links.get(rel="foo") == link_a
        assert links.get(type="text/html") == link_a

        # get with raising=True raises when multiple links match.
        with pytest.raises(
            PalaceValueError,
            match="^Multiple links found with rel='foo' and type='text/html'$",
        ):
            links.get(rel="foo", type="text/html", raising=True)

        # Single-match queries still work.
        assert links.get(rel="bar") == link_c
        assert links.get(rel="bar", raising=True) == link_c


class TestValidateUniqueLinks:
    def test_accepts_unique_links(self) -> None:
        links = CompactCollection(
            [
                BaseLink(href="http://example.com/a", rel="foo"),
                BaseLink(href="http://example.com/b", rel="bar"),
            ]
        )
        assert validate_unique_links(links) is links

    def test_rejects_exact_duplicates(self) -> None:
        link = BaseLink(href="http://example.com/a", rel="foo", type="text/html")
        links = CompactCollection([link, link])
        with pytest.raises(PalaceValueError, match="Duplicate link"):
            validate_unique_links(links)

    def test_accepts_links_sharing_rel_type_href(self) -> None:
        """Links with same (rel, type, href) but differing in other fields
        are not duplicates under full object equality."""
        link_a = BaseLink(href="http://example.com/a", rel="foo", type="text/html")
        link_b = BaseLink(
            href="http://example.com/a", rel="foo", type="text/html", templated=True
        )
        links = CompactCollection([link_a, link_b])
        assert validate_unique_links(links) is links
