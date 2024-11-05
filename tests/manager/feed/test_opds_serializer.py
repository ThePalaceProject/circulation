import datetime
from unittest.mock import patch

import pytz
from lxml import etree

from palace.manager.feed.serializer.opds import (
    OPDS1Version1Serializer,
    OPDS1Version2Serializer,
)
from palace.manager.feed.serializer.opds2 import PALACE_REL_SORT
from palace.manager.feed.types import (
    Acquisition,
    Author,
    FeedData,
    FeedEntryType,
    IndirectAcquisition,
    Link,
    WorkEntryData,
)
from palace.manager.util.opds_writer import OPDSFeed, OPDSMessage


class TestOPDSSerializer:
    def test__serialize_feed_entry(self):
        grandchild = FeedEntryType.create(text="grandchild", attr="gcattr")
        child = FeedEntryType.create(text="child", attr="chattr", grandchild=grandchild)
        parent = FeedEntryType.create(text="parent", attr="pattr", child=child)

        serialized = OPDS1Version1Serializer()._serialize_feed_entry("parent", parent)

        assert serialized.tag == "parent"
        assert serialized.text == "parent"
        assert serialized.get("attr") == "pattr"
        children = list(serialized)
        assert len(children) == 1
        assert children[0].tag == "child"
        assert children[0].text == "child"
        assert children[0].get("attr") == "chattr"
        children = list(children[0])
        assert len(children) == 1
        assert children[0].tag == "grandchild"
        assert children[0].text == "grandchild"
        assert children[0].get("attr") == "gcattr"

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

    def test__serialize_acquistion_link(self):
        link = Acquisition(
            href="http://acquisition",
            holds_total="0",
            copies_total="1",
            availability_status="available",
            indirect_acquisitions=[IndirectAcquisition(type="indirect")],
            lcp_hashed_passphrase=FeedEntryType(text="passphrase"),
            drm_licensor=FeedEntryType.create(
                vendor="vendor", clientToken=FeedEntryType(text="token")
            ),
        )
        element = OPDS1Version1Serializer()._serialize_acquistion_link(link)
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

    def test_serialize_work_entry(self):
        data = WorkEntryData(
            additionalType="type",
            identifier="identifier",
            pwid="permanent-work-id",
            summary=FeedEntryType(text="summary"),
            language=FeedEntryType(text="language"),
            publisher=FeedEntryType(text="publisher"),
            issued=datetime.datetime(2020, 2, 2, tzinfo=pytz.UTC),
            published=FeedEntryType(text="published"),
            updated=FeedEntryType(text="updated"),
            title=FeedEntryType(text="title"),
            subtitle=FeedEntryType(text="subtitle"),
            series=FeedEntryType.create(
                name="series",
                link=Link(href="http://series", title="series title", rel="series"),
            ),
            imprint=FeedEntryType(text="imprint"),
            authors=[Author(name="author")],
            contributors=[Author(name="contributor")],
            categories=[
                FeedEntryType.create(scheme="scheme", term="term", label="label")
            ],
            ratings=[FeedEntryType(text="rating")],
            duration=10,
        )

        element = OPDS1Version1Serializer().serialize_work_entry(data)

        assert (
            element.get(f"{{{OPDSFeed.SCHEMA_NS}}}additionalType")
            == data.additionalType
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
        assert child[0].text == data.language.text

        child = element.findall(f"{{{OPDSFeed.DCTERMS_NS}}}publisher")
        assert len(child) == 1
        assert child[0].text == data.publisher.text

        child = element.findall(f"{{{OPDSFeed.DCTERMS_NS}}}issued")
        assert len(child) == 1
        assert child[0].text == data.issued.date().isoformat()

        child = element.findall(f"published")
        assert len(child) == 1
        assert child[0].text == data.published.text

        child = element.findall(f"updated")
        assert len(child) == 1
        assert child[0].text == data.updated.text

        child = element.findall(f"title")
        assert len(child) == 1
        assert child[0].text == data.title.text

        child = element.findall(f"{{{OPDSFeed.SCHEMA_NS}}}alternativeHeadline")
        assert len(child) == 1
        assert child[0].text == data.subtitle.text

        child = element.findall(f"{{{OPDSFeed.SCHEMA_NS}}}series")
        assert len(child) == 1
        assert child[0].get("name") == getattr(data.series, "name")
        link = list(child[0])[0]
        assert link.tag == "link"
        assert link.get("title") == "series title"
        assert link.get("href") == "http://series"

        child = element.findall(f"{{{OPDSFeed.BIB_SCHEMA_NS}}}publisherImprint")
        assert len(child) == 1
        assert child[0].text == data.imprint.text

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
        assert child[0].text == data.ratings[0].text

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
        sort_link_input = Link(href="test", rel="test_rel", title="text1")
        sort_link_input.add_attributes(
            dict(facetGroup="Sort by", activeFacet="true", defaultFacet="true")
        )

        facet_link = Link(href="test", rel="test_rel", title="text1")
        facet_link.add_attributes(
            dict(facetGroup="non_sort_group", activeFacet="true", defaultFacet="true")
        )

        serializer = OPDS1Version2Serializer()
        feed = FeedData()
        feed.facet_links = [sort_link_input, facet_link]

        sort_links = serializer._serialize_sort_links(feed)
        # we expect only the sort link to be returned.
        assert len(sort_links) == 1
        sort_link = sort_links[0]
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

        with patch.object(serializer, "_serialize_sort_links") as serialize_sort_links:

            serializer.serialize_feed(feed)
            assert serialize_sort_links.call_count == 1

    def test_serialize_non_sort_facetgroup_link_v2(self):
        facet_link = Link(href="test", rel="test_rel", title="text1")
        facet_link.add_attributes(
            dict(facetGroup="non_sort_group", activeFacet="true", defaultFacet="true")
        )

        sort_link = Link(href="test", rel="test_rel", title="text1")
        sort_link.add_attributes(
            dict(facetGroup="Sort by", activeFacet="true", defaultFacet="true")
        )
        serializer = OPDS1Version2Serializer()
        feed = FeedData()
        feed.facet_links = [facet_link, sort_link]
        facet_links = serializer._serialize_facet_links(feed)

        # we expect only the non sort facet links to be returned.
        assert len(facet_links) == 1
        facet_link = facet_links[0]
        assert facet_link.attrib["title"] == "text1"
        assert facet_link.attrib["href"] == "test"
        assert facet_link.attrib["rel"] == "test_rel"
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
        sort_link_input = Link(href="test", rel="test_rel", title="text1")
        sort_link_input.add_attributes(
            dict(facetGroup="Sort by", activeFacet="true", defaultFacet="true")
        )

        facet_link = Link(href="test", rel="test_rel", title="text1")
        facet_link.add_attributes(
            dict(facetGroup="non_sort_group", activeFacet="true", defaultFacet="true")
        )

        serializer = OPDS1Version1Serializer()
        feed = FeedData()
        feed.facet_links = [sort_link_input, facet_link]

        sort_links = serializer._serialize_sort_links(feed)
        # we expect no sort links to be returned
        assert len(sort_links) == 0

        # and two facet links:
        facet_links = serializer._serialize_facet_links(feed)
        assert len(facet_links) == 2

        sort_link = facet_links[0]
        assert sort_link.attrib["title"] == "text1"
        assert sort_link.attrib["href"] == "test"
        assert sort_link.attrib["rel"] == "test_rel"
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
        assert facet_link.attrib["rel"] == "test_rel"
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
