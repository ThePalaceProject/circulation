import datetime

from lxml import etree

from palace.manager.core.user_profile import ProfileController
from palace.manager.feed.serializer.opds import (
    OPDS1Version1Serializer,
    OPDS1Version2Serializer,
)
from palace.manager.feed.serializer.opds2 import PALACE_REL_SORT
from palace.manager.feed.types import (
    Acquisition,
    Author,
    Category,
    DRMLicensor,
    FacetData,
    FeedData,
    FeedEntryGroup,
    FeedMetadata,
    IndirectAcquisition,
    Link,
    LinkContentType,
    Rating,
    RichText,
    Series,
    WorkEntry,
    WorkEntryData,
)
from palace.manager.sqlalchemy.constants import LinkRelations
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.opds_writer import OPDSFeed, OPDSMessage


class TestOPDSSerializer:
    def test__serialize_link(self):
        link = Link(
            href="http://link",
            rel="rel",
            type="type",
            title="title",
            role="role",
            active_facet=True,
        )

        serialized = OPDS1Version1Serializer()._serialize_link(link)

        assert serialized.tag == "link"
        assert serialized.get("href") == "http://link"
        assert serialized.get("rel") == "rel"
        assert serialized.get("type") == "type"
        assert serialized.get("title") == "title"
        assert serialized.get("role") == "role"
        assert serialized.get(f"{{{OPDSFeed.OPDS_NS}}}activeFacet") == "true"
        # Group-level attributes are not set by _serialize_link (use _serialize_facet_link)
        assert serialized.get(f"{{{OPDSFeed.OPDS_NS}}}facetGroup") is None
        assert serialized.get(f"{{{OPDSFeed.SIMPLIFIED_NS}}}facetGroupType") is None

    def test__serialize_facet_link(self):
        """_serialize_facet_link adds group attributes and facet rel from FacetData."""
        link = Link(href="http://link", active_facet=True)
        facet_data = FacetData(group="Group", type="entrypoint")

        serialized = OPDS1Version1Serializer()._serialize_facet_link(link, facet_data)

        assert serialized.get("rel") == LinkRelations.FACET_REL
        assert serialized.get(f"{{{OPDSFeed.OPDS_NS}}}facetGroup") == "Group"
        assert (
            serialized.get(f"{{{OPDSFeed.SIMPLIFIED_NS}}}facetGroupType")
            == "entrypoint"
        )
        assert serialized.get(f"{{{OPDSFeed.OPDS_NS}}}activeFacet") == "true"

    def test__serialize_author_tag(self):
        author = Author(
            name="Author",
            sort_name="sort_name",
            role="role",
            link=Link(href="http://author", title="link title"),
            viaf="viaf",
            family_name="family name",
            wikipedia_name="wiki name",
            lc="lc",
        )

        element = OPDS1Version1Serializer()._serialize_author_tag("author", author)

        assert element.tag == "author"
        assert element.get(f"{{{OPDSFeed.OPF_NS}}}role") == author.role

        expected_child_tags = [
            (f"{{{OPDSFeed.ATOM_NS}}}name", author.name, None),
            (f"{{{OPDSFeed.SIMPLIFIED_NS}}}sort_name", author.sort_name, None),
            (
                f"{{{OPDSFeed.SIMPLIFIED_NS}}}wikipedia_name",
                author.wikipedia_name,
                None,
            ),
            ("sameas", author.viaf, None),
            ("sameas", author.lc, None),
            ("link", None, dict(href=author.link.href, title=author.link.title)),
        ]

        child: etree._Element
        for expect in expected_child_tags:
            tag, text, attrs = expect

            # element.find is not working for "link" :|
            for child in element:
                if child.tag == tag:
                    break
            else:
                assert False, f"Did not find {expect}"

            # Remove the element so we don't find it again
            element.remove(child)

            # Assert the data
            assert child.text == text
            if attrs:
                assert dict(child.attrib) == attrs

        # No more children
        assert list(element) == []

    def test__serialize_acquisition_link(self):
        link = Acquisition(
            href="http://acquisition",
            holds_total="0",
            copies_total="1",
            availability_status="available",
            indirect_acquisitions=[IndirectAcquisition(type="indirect")],
            lcp_hashed_passphrase="passphrase",
            drm_licensor=DRMLicensor(vendor="vendor", client_token="token"),
        )
        element = OPDS1Version1Serializer()._serialize_acquisition_link(link)
        assert element.tag == "link"
        assert dict(element.attrib) == dict(href=link.href)

        tests = [
            (
                f"{{{OPDSFeed.OPDS_NS}}}indirectAcquisition",
                lambda child: child.get("type") == "indirect",
            ),
            (f"{{{OPDSFeed.OPDS_NS}}}holds", lambda child: child.get("total") == "0"),
            (f"{{{OPDSFeed.OPDS_NS}}}copies", lambda child: child.get("total") == "1"),
            (
                f"{{{OPDSFeed.OPDS_NS}}}availability",
                lambda child: child.get("status") == "available",
            ),
            (
                f"{{{OPDSFeed.LCP_NS}}}hashed_passphrase",
                lambda child: child.text == "passphrase",
            ),
            (
                f"{{{OPDSFeed.DRM_NS}}}licensor",
                lambda child: child.get(f"{{{OPDSFeed.DRM_NS}}}vendor") == "vendor"
                and child[0].text == "token",
            ),
        ]
        for tag, test_fn in tests:
            children = element.findall(tag)
            assert len(children) == 1
            assert test_fn(children[0])

        # Test serializing a templated link
        link.templated = True
        link.href = "http://templated.acquisition/{?foo,bar}"
        element = OPDS1Version1Serializer()._serialize_acquisition_link(link)
        assert element.tag == "{http://drafts.opds.io/odl-1.0#}tlink"
        assert element.get("href") == link.href

    def test_serialize_work_entry(self):
        data = WorkEntryData(
            medium="Book",
            identifier="identifier",
            pwid="permanent-work-id",
            summary=RichText(text="summary"),
            language="language",
            publisher="publisher",
            issued=datetime.datetime(2020, 2, 2, tzinfo=datetime.UTC),
            published="published",
            updated="updated",
            title="title",
            subtitle="subtitle",
            series=Series(
                name="series",
                link=Link(href="http://series", title="series title", rel="series"),
            ),
            imprint="imprint",
            authors=[Author(name="author")],
            contributors=[Author(name="contributor")],
            categories=[Category(scheme="scheme", term="term", label="label")],
            ratings=[Rating(rating_value="rating")],
            duration=10,
        )

        element = OPDS1Version1Serializer().serialize_work_entry(data)

        assert (
            element.get(f"{{{OPDSFeed.SCHEMA_NS}}}additionalType")
            == "http://schema.org/EBook"
        )

        child = element.xpath(f"id")
        assert len(child) == 1
        assert child[0].text == data.identifier

        child = element.findall(f"{{{OPDSFeed.SIMPLIFIED_NS}}}pwid")
        assert len(child) == 1
        assert child[0].text == data.pwid

        child = element.xpath("summary")
        assert len(child) == 1
        assert child[0].text == data.summary.text

        child = element.findall(f"{{{OPDSFeed.DCTERMS_NS}}}language")
        assert len(child) == 1
        assert child[0].text == data.language

        child = element.findall(f"{{{OPDSFeed.DCTERMS_NS}}}publisher")
        assert len(child) == 1
        assert child[0].text == data.publisher

        child = element.findall(f"{{{OPDSFeed.DCTERMS_NS}}}issued")
        assert len(child) == 1
        assert child[0].text == data.issued.date().isoformat()

        child = element.findall(f"published")
        assert len(child) == 1
        assert child[0].text == data.published

        child = element.findall(f"updated")
        assert len(child) == 1
        assert child[0].text == data.updated

        child = element.findall(f"title")
        assert len(child) == 1
        assert child[0].text == data.title

        child = element.findall(f"{{{OPDSFeed.SCHEMA_NS}}}alternativeHeadline")
        assert len(child) == 1
        assert child[0].text == data.subtitle

        child = element.findall(f"{{{OPDSFeed.SCHEMA_NS}}}series")
        assert len(child) == 1
        assert child[0].get("name") == getattr(data.series, "name")
        link = list(child[0])[0]
        assert link.tag == "link"
        assert link.get("title") == "series title"
        assert link.get("href") == "http://series"

        child = element.findall(f"{{{OPDSFeed.BIB_SCHEMA_NS}}}publisherImprint")
        assert len(child) == 1
        assert child[0].text == data.imprint

        child = element.findall(f"author")
        assert len(child) == 1
        name_tag = list(child[0])[0]
        assert name_tag.tag == f"{{{OPDSFeed.ATOM_NS}}}name"
        assert name_tag.text == "author"

        child = element.findall(f"contributor")
        assert len(child) == 1
        name_tag = list(child[0])[0]
        assert name_tag.tag == f"{{{OPDSFeed.ATOM_NS}}}name"
        assert name_tag.text == "contributor"

        child = element.findall(f"category")
        assert len(child) == 1
        assert child[0].get("scheme") == "scheme"
        assert child[0].get("term") == "term"
        assert child[0].get("label") == "label"

        child = element.findall(f"Rating")
        assert len(child) == 1
        assert (
            child[0].get(f"{{{OPDSFeed.SCHEMA_NS}}}ratingValue")
            == data.ratings[0].rating_value
        )

        child = element.findall(f"{{{OPDSFeed.DCTERMS_NS}}}duration")
        assert len(child) == 1
        assert child[0].text == "10"

    def test_serialize_work_entry_empty(self):
        # A no-data work entry
        element = OPDS1Version1Serializer().serialize_work_entry(WorkEntryData())
        # This will create an empty <entry> tag
        assert element.tag == "entry"
        assert list(element) == []

    def test_serialize_opds_message(self):
        message = OPDSMessage("URN", 200, "Description")
        serializer = OPDS1Version1Serializer()
        result = serializer.serialize_opds_message(message)
        assert serializer.to_string(result) == serializer.to_string(message.tag)

    def test_serialize_sort_link_v2(self):
        sort_link_input = Link(
            href="test",
            rel="test_rel",
            title="text1",
            active_facet=True,
            default_facet=True,
        )

        facet_link = Link(
            href="test",
            rel="test_rel",
            title="text1",
            active_facet=True,
            default_facet=True,
        )

        serializer = OPDS1Version2Serializer()
        feed = FeedData()
        feed.facets = [
            FacetData(group="Sort by", type=PALACE_REL_SORT, links=[sort_link_input]),
            FacetData(group="non_sort_group", links=[facet_link]),
        ]

        facet_links = serializer._serialize_facet_links(feed)

        # Sort link comes first, then the non-sort facet link.
        assert len(facet_links) == 2
        sort_link = facet_links[0]
        assert sort_link.attrib["title"] == "text1"
        assert sort_link.attrib["href"] == "test"
        assert sort_link.attrib["rel"] == PALACE_REL_SORT
        assert (
            sort_link.attrib["{http://palaceproject.io/terms/properties/}active-sort"]
            == "true"
        )
        assert (
            sort_link.attrib["{http://palaceproject.io/terms/properties/}default"]
            == "true"
        )

    def test_serialize_non_sort_facetgroup_link_v2(self):
        facet_link_data = Link(
            href="test",
            rel="test_rel",
            title="text1",
            active_facet=True,
            default_facet=True,
        )

        sort_link = Link(
            href="test",
            rel="test_rel",
            title="text1",
            active_facet=True,
            default_facet=True,
        )
        serializer = OPDS1Version2Serializer()
        feed = FeedData()
        feed.facets = [
            FacetData(group="non_sort_group", links=[facet_link_data]),
            FacetData(group="Sort by", type=PALACE_REL_SORT, links=[sort_link]),
        ]
        facet_links = serializer._serialize_facet_links(feed)

        # Both the non-sort facet and the sort link are returned.
        assert len(facet_links) == 2
        facet_link = facet_links[0]
        assert facet_link.attrib["title"] == "text1"
        assert facet_link.attrib["href"] == "test"
        assert facet_link.attrib["rel"] == LinkRelations.FACET_REL
        assert (
            facet_link.attrib["{http://opds-spec.org/2010/catalog}activeFacet"]
            == "true"
        )
        assert (
            facet_link.attrib["{http://palaceproject.io/terms/properties/}default"]
            == "true"
        )

        assert (
            facet_link.attrib["{http://opds-spec.org/2010/catalog}facetGroup"]
            == "non_sort_group"
        )

    def test_serialize_facets_and_sort_links_v1(self):
        sort_link_input = Link(
            href="test",
            rel="test_rel",
            title="text1",
            active_facet=True,
            default_facet=True,
        )

        facet_link_data = Link(
            href="test",
            rel="test_rel",
            title="text1",
            active_facet=True,
            default_facet=True,
        )

        serializer = OPDS1Version1Serializer()
        feed = FeedData()
        feed.facets = [
            FacetData(group="Sort by", type=PALACE_REL_SORT, links=[sort_link_input]),
            FacetData(group="non_sort_group", links=[facet_link_data]),
        ]

        # V1 treats all facets uniformly -- two facet links:
        facet_links = serializer._serialize_facet_links(feed)
        assert len(facet_links) == 2

        sort_link = facet_links[0]
        assert sort_link.attrib["title"] == "text1"
        assert sort_link.attrib["href"] == "test"
        assert sort_link.attrib["rel"] == LinkRelations.FACET_REL
        assert (
            sort_link.attrib["{http://opds-spec.org/2010/catalog}activeFacet"] == "true"
        )
        assert (
            sort_link.attrib["{http://opds-spec.org/2010/catalog}facetGroup"]
            == "Sort by"
        )

        assert (
            "{http://palaceproject.io/terms/properties/}default" not in sort_link.attrib
        )

        facet_link = facet_links[1]
        assert facet_link.attrib["title"] == "text1"
        assert facet_link.attrib["href"] == "test"
        assert facet_link.attrib["rel"] == LinkRelations.FACET_REL
        assert (
            facet_link.attrib["{http://opds-spec.org/2010/catalog}activeFacet"]
            == "true"
        )
        assert (
            facet_link.attrib["{http://opds-spec.org/2010/catalog}facetGroup"]
            == "non_sort_group"
        )

        assert (
            "{http://palaceproject.io/terms/properties/}default"
            not in facet_link.attrib
        )

    def test_serialize_work_entry_with_subtitle_equals_none(self):
        data = WorkEntryData(
            subtitle=None,
        )

        element = OPDS1Version1Serializer().serialize_work_entry(data)
        child = element.findall(f"{{{OPDSFeed.SCHEMA_NS}}}alternativeHeadline")
        assert len(child) == 0

        data = WorkEntryData(
            subtitle="test",
        )

        element = OPDS1Version1Serializer().serialize_work_entry(data)
        child = element.findall(f"{{{OPDSFeed.SCHEMA_NS}}}alternativeHeadline")
        assert len(child) == 1
        assert child[0].text == "test"

    def test_resolve_type_maps_link_content_types(self):
        """LinkContentType values are mapped to OPDS1-specific content types."""
        serializer = OPDS1Version1Serializer()
        assert (
            serializer._resolve_type(LinkContentType.OPDS_FEED)
            == OPDSFeed.ACQUISITION_FEED_TYPE
        )
        assert (
            serializer._resolve_type(LinkContentType.OPDS_ENTRY) == OPDSFeed.ENTRY_TYPE
        )

    def test_resolve_type_passes_through_concrete_types(self):
        """Concrete content types are passed through unchanged."""
        serializer = OPDS1Version1Serializer()
        assert serializer._resolve_type("text/html") == "text/html"
        assert serializer._resolve_type("application/json") == "application/json"
        assert serializer._resolve_type(None) is None

    def test_resolve_rel_maps_profile(self):
        """Standard 'profile' rel is mapped to Palace-specific profile rel."""
        serializer = OPDS1Version1Serializer()
        assert serializer._resolve_rel("profile") == ProfileController.LINK_RELATION

    def test_resolve_rel_passes_through_other_rels(self):
        """Non-mapped rels are passed through unchanged."""
        serializer = OPDS1Version1Serializer()
        assert serializer._resolve_rel("self") == "self"
        assert serializer._resolve_rel("alternate") == "alternate"
        assert serializer._resolve_rel(None) is None

    def test_serialize_link_resolves_link_content_type(self):
        """_serialize_link resolves LinkContentType to OPDS1 type."""
        link = Link(
            href="http://example.com/feed",
            rel="http://opds-spec.org/shelf",
            type=LinkContentType.OPDS_FEED,
        )
        serializer = OPDS1Version1Serializer()
        element = serializer._serialize_link(link)
        assert element.get("type") == OPDSFeed.ACQUISITION_FEED_TYPE

    def test_serialize_link_resolves_profile_rel(self):
        """_serialize_link resolves standard 'profile' rel to Palace-specific rel."""
        link = Link(
            href="http://example.com/profile",
            rel="profile",
        )
        serializer = OPDS1Version1Serializer()
        element = serializer._serialize_link(link)
        assert element.get("rel") == ProfileController.LINK_RELATION

    def test_serialize_acquisition_link_resolves_link_content_type(self):
        """_serialize_acquisition_link resolves LinkContentType to OPDS1 type."""
        link = Acquisition(
            href="http://example.com/borrow",
            rel="http://opds-spec.org/acquisition/borrow",
            type=LinkContentType.OPDS_ENTRY,
        )
        serializer = OPDS1Version1Serializer()
        element = serializer._serialize_acquisition_link(link)
        assert element.get("type") == OPDSFeed.ENTRY_TYPE

    def test_serialize_acquisition_link_resolves_indirect_content_type(self):
        """Indirect acquisition LinkContentType values are resolved to OPDS1 types."""
        link = Acquisition(
            href="http://example.com/borrow",
            rel="http://opds-spec.org/acquisition/borrow",
            indirect_acquisitions=[
                IndirectAcquisition(type=LinkContentType.OPDS_ENTRY)
            ],
        )
        serializer = OPDS1Version1Serializer()
        element = serializer._serialize_acquisition_link(link)
        indirect = element.find(f"{{{OPDSFeed.OPDS_NS}}}indirectAcquisition")
        assert indirect is not None
        assert indirect.get("type") == OPDSFeed.ENTRY_TYPE

    def test_entry_groups_produce_flat_entries_with_collection_links(self):
        """entry_groups in FeedData produce flat entries with rel='collection' links in OPDS1."""
        entry1 = WorkEntry(work=Work(), edition=Edition(), identifier=Identifier())
        entry1.computed = WorkEntryData(
            identifier="urn:1",
            title="Book One",
        )

        entry2 = WorkEntry(work=Work(), edition=Edition(), identifier=Identifier())
        entry2.computed = WorkEntryData(
            identifier="urn:2",
            title="Book Two",
        )

        feed = FeedData(
            metadata=FeedMetadata(title="Grouped Feed"),
            entry_groups=[
                FeedEntryGroup(
                    href="http://group/space-opera",
                    title="Space Opera",
                    entries=[entry1],
                ),
                FeedEntryGroup(
                    href="http://group/cyberpunk",
                    title="Cyberpunk",
                    entries=[entry2],
                ),
            ],
        )

        serializer = OPDS1Version1Serializer()
        result = serializer.serialize_feed(feed)
        root = etree.fromstring(result.encode())

        # There should be 2 entry elements.
        entries = root.findall(f"{{{OPDSFeed.ATOM_NS}}}entry")
        assert len(entries) == 2

        # First entry should have a collection link for Space Opera.
        links1 = entries[0].findall(f"{{{OPDSFeed.ATOM_NS}}}link")
        collection_links1 = [l for l in links1 if l.get("rel") == OPDSFeed.GROUP_REL]
        assert len(collection_links1) == 1
        assert collection_links1[0].get("href") == "http://group/space-opera"
        assert collection_links1[0].get("title") == "Space Opera"

        # Second entry should have a collection link for Cyberpunk.
        links2 = entries[1].findall(f"{{{OPDSFeed.ATOM_NS}}}link")
        collection_links2 = [l for l in links2 if l.get("rel") == OPDSFeed.GROUP_REL]
        assert len(collection_links2) == 1
        assert collection_links2[0].get("href") == "http://group/cyberpunk"
        assert collection_links2[0].get("title") == "Cyberpunk"

    def test_entry_groups_and_flat_entries_coexist(self):
        """Both entry_groups and flat entries can coexist in the same feed."""
        grouped_entry = WorkEntry(
            work=Work(), edition=Edition(), identifier=Identifier()
        )
        grouped_entry.computed = WorkEntryData(
            identifier="urn:grouped",
            title="Grouped Book",
        )

        flat_entry = WorkEntry(work=Work(), edition=Edition(), identifier=Identifier())
        flat_entry.computed = WorkEntryData(
            identifier="urn:flat",
            title="Flat Book",
        )

        feed = FeedData(
            metadata=FeedMetadata(title="Mixed Feed"),
            entry_groups=[
                FeedEntryGroup(
                    href="http://group/test",
                    title="Test Group",
                    entries=[grouped_entry],
                ),
            ],
            entries=[flat_entry],
        )

        serializer = OPDS1Version1Serializer()
        result = serializer.serialize_feed(feed)
        root = etree.fromstring(result.encode())

        entries = root.findall(f"{{{OPDSFeed.ATOM_NS}}}entry")
        assert len(entries) == 2

        # First entry (from group) has a collection link.
        grouped_links = [
            l
            for l in entries[0].findall(f"{{{OPDSFeed.ATOM_NS}}}link")
            if l.get("rel") == OPDSFeed.GROUP_REL
        ]
        assert len(grouped_links) == 1

        # Second entry (flat) has no collection link.
        flat_links = [
            l
            for l in entries[1].findall(f"{{{OPDSFeed.ATOM_NS}}}link")
            if l.get("rel") == OPDSFeed.GROUP_REL
        ]
        assert len(flat_links) == 0
