import json
from unittest.mock import patch

from palace.manager.feed.serializer.opds2 import (
    PALACE_PROPERTIES_ACTIVE_SORT,
    PALACE_PROPERTIES_DEFAULT,
    PALACE_REL_SORT,
    OPDS2Serializer,
)
from palace.manager.feed.types import (
    Acquisition,
    Author,
    Category,
    DataEntry,
    DataEntryTypes,
    DRMLicensor,
    FeedData,
    FeedMetadata,
    IndirectAcquisition,
    Link,
    LinkContentType,
    RichText,
    Series,
    WorkEntry,
    WorkEntryData,
)
from palace.manager.opds import opds2
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.opds_writer import OPDSFeed, OPDSMessage

OPDS2_CONTENT_TYPE = OPDS2Serializer.content_type()


class TestOPDS2Serializer:
    def test_serialize_feed(self):
        feed = FeedData(
            metadata=FeedMetadata(
                title="Title",
                items_per_page=20,
                id="http://feed",
            )
        )
        w = WorkEntry(
            work=Work(),
            edition=Edition(),
            identifier=Identifier(),
        )
        w.computed = WorkEntryData(
            identifier="identifier",
            pwid="permanent-id",
            title="Work Title",
            medium="Book",
            image_links=[
                Link(
                    href="http://image",
                    rel=OPDSFeed.FULL_IMAGE_REL,
                    type="image/png",
                )
            ],
            acquisition_links=[
                Acquisition(
                    href="http://acquisition",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
        )
        feed.entries = [w]
        feed.links = [Link(href="http://link", rel="self")]
        feed.facet_links = [
            Link(
                href="http://facet-link-1",
                rel="facet-rel",
                title="Facet One",
                facet_group="FacetGroup",
            ),
            Link(
                href="http://facet-link-2",
                rel="facet-rel",
                title="Facet Two",
                facet_group="FacetGroup",
            ),
        ]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)

        assert result["metadata"]["title"] == "Title"
        assert result["metadata"]["itemsPerPage"] == 20

        assert len(result["publications"]) == 1
        assert result["publications"][0] == dict(
            metadata={
                "identifier": "identifier",
                "@type": "http://schema.org/Book",
                "title": "Work Title",
            },
            images=[
                {
                    "href": "http://image",
                    "rel": OPDSFeed.FULL_IMAGE_REL,
                    "type": "image/png",
                }
            ],
            links=[
                {
                    "href": "http://acquisition",
                    "rel": OPDSFeed.OPEN_ACCESS_REL,
                    "type": "application/epub+zip",
                }
            ],
        )

        assert len(result["links"]) == 1
        assert result["links"][0] == dict(
            href="http://link",
            rel="self",
            type=OPDS2_CONTENT_TYPE,
        )

        assert len(result["facets"]) == 1
        assert result["facets"][0] == dict(
            metadata={"title": "FacetGroup"},
            links=[
                {
                    "href": "http://facet-link-1",
                    "rel": "facet-rel",
                    "title": "Facet One",
                },
                {
                    "href": "http://facet-link-2",
                    "rel": "facet-rel",
                    "title": "Facet Two",
                },
            ],
        )

    def test_serialize_work_entry(self):
        data = WorkEntryData(
            medium="Book",
            title="The Title",
            sort_title="Title, The",
            subtitle="Sub Title",
            identifier="urn:id",
            language="de",
            updated="2022-02-02T00:00:00Z",
            published="2020-02-02T00:00:00Z",
            summary=RichText(text="Summary"),
            publisher="Publisher",
            imprint="Imprint",
            categories=[
                Category(scheme="scheme", term="label", label="label"),
            ],
            series=Series(name="Series", position=3),
            image_links=[
                Link(
                    href="http://image",
                    rel=OPDSFeed.FULL_IMAGE_REL,
                    type="image/png",
                )
            ],
            acquisition_links=[
                Acquisition(
                    href="http://acquisition",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            other_links=[Link(href="http://link", rel="rel", type="text/html")],
            duration=10,
        )

        serializer = OPDS2Serializer()

        entry = serializer.serialize_work_entry(data)
        metadata = entry["metadata"]

        assert metadata["@type"] == "http://schema.org/Book"
        assert metadata["title"] == data.title
        assert metadata["sortAs"] == data.sort_title
        assert metadata["duration"] == data.duration
        assert metadata["subtitle"] == data.subtitle
        assert metadata["identifier"] == data.identifier
        assert metadata["language"] == data.language
        assert metadata["modified"] == data.updated
        assert metadata["published"] == data.published
        assert metadata["description"] == data.summary.text
        assert metadata["publisher"] == dict(name=data.publisher)
        assert metadata["imprint"] == dict(name=data.imprint)
        assert metadata["subject"] == [
            dict(scheme="scheme", code="label", name="label", sortAs="label")
        ]
        assert metadata["belongsTo"] == dict(series={"name": "Series", "position": 3})

        assert entry["links"] == [
            dict(href="http://link", rel="rel", type="text/html"),
            dict(
                href="http://acquisition",
                rel=OPDSFeed.OPEN_ACCESS_REL,
                type="application/epub+zip",
            ),
        ]
        assert entry["images"] == [
            dict(
                href="http://image",
                rel=OPDSFeed.FULL_IMAGE_REL,
                type="image/png",
            )
        ]

        # Test the different author types
        data = WorkEntryData(
            medium="Book",
            title="Author Work",
            identifier="urn:id",
            image_links=[
                Link(
                    href="http://image",
                    rel=OPDSFeed.FULL_IMAGE_REL,
                    type="image/png",
                )
            ],
            acquisition_links=[
                Acquisition(
                    href="http://acquisition",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            authors=[Author(name="author1"), Author(name="author2")],
            contributors=[
                Author(name="translator", role="trl"),
                Author(name="editor", role="edt"),
                Author(name="artist", role="art"),
                Author(name="illustrator", role="ill"),
                Author(name="letterer", role="ctb"),
                Author(name="penciller", role="ctb"),
                Author(name="colorist", role="clr"),
                Author(name="inker", role="ctb"),
                Author(name="narrator", role="nrt"),
                Author(name="narrator2", role="nrt"),
            ],
        )

        entry = serializer.serialize_work_entry(data)
        metadata = entry["metadata"]
        assert metadata["author"] == [dict(name="author1"), dict(name="author2")]
        # Of the allowed roles
        assert metadata["translator"] == dict(name="translator")
        assert metadata["editor"] == dict(name="editor")
        assert metadata["artist"] == dict(name="artist")
        assert metadata["illustrator"] == dict(name="illustrator")
        assert metadata["colorist"] == dict(name="colorist")
        # Of letterer, penciller, and inker, only inker is used, since the marc roles overlap
        assert metadata["inker"] == dict(name="inker")
        # Of repeated roles, only the last entry is picked
        assert metadata["narrator"] == dict(name="narrator2")

    def test__serialize_acquisition_link(self):
        drm_licensor = DRMLicensor(vendor="vendor_name", client_token="token_value")

        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://acquisition",
            rel="acquisition",
            type="html",
            availability_status="available",
            availability_since="2022-02-02T00:00:00Z",
            availability_until="2222-02-02T00:00:00Z",
            lcp_hashed_passphrase="LCPPassphrase",
            indirect_acquisitions=[
                IndirectAcquisition(
                    type="indirect1",
                    children=[
                        IndirectAcquisition(type="indirect1-1"),
                        IndirectAcquisition(type="indirect1-2"),
                    ],
                ),
            ],
            drm_licensor=drm_licensor,
        )

        result = serializer._dump_model(
            serializer._serialize_acquisition_link(acquisition)
        )

        assert result["href"] == acquisition.href
        assert result["rel"] == acquisition.rel
        assert result["type"] == acquisition.type
        assert result["properties"] == dict(
            availability={
                "since": "2022-02-02T00:00:00Z",
                "until": "2222-02-02T00:00:00Z",
                "state": "available",
            },
            indirectAcquisition=[
                {
                    "type": "indirect1",
                    "child": [{"type": "indirect1-1"}, {"type": "indirect1-2"}],
                }
            ],
            lcp_hashed_passphrase="LCPPassphrase",
            licensor={"clientToken": "token_value", "vendor": "vendor_name"},
        )

        # Test availability states
        acquisition = Acquisition(
            href="http://hold",
            rel="hold",
            is_hold=True,
            availability_status="available",
            type="application/epub+zip",
        )
        result = serializer._dump_model(
            serializer._serialize_acquisition_link(acquisition)
        )
        assert result["properties"]["availability"]["state"] == "reserved"

        acquisition = Acquisition(
            href="http://loan",
            rel="loan",
            is_loan=True,
            availability_status="available",
            type="application/epub+zip",
        )
        result = serializer._dump_model(
            serializer._serialize_acquisition_link(acquisition)
        )
        assert result["properties"]["availability"]["state"] == "ready"

        # Test templated link
        acquisition = Acquisition(
            href="http://templated.acquisition/{?foo,bar}",
            templated=True,
            type="application/epub+zip",
        )
        result = serializer._dump_model(
            serializer._serialize_acquisition_link(acquisition)
        )
        assert result["templated"] is True
        assert result["href"] == acquisition.href

    def test__serialize_contributor(self):
        author = Author(
            name="Author",
            sort_name="Author,",
            link=Link(href="http://author", rel="contributor", title="Delete me!"),
        )
        serializer = OPDS2Serializer()
        result = serializer._dump_model(serializer._serialize_contributor(author))
        assert result["name"] == "Author"
        assert result["sortAs"] == "Author,"
        assert result["links"] == [{"href": "http://author", "rel": "contributor"}]

    def test__serialize_contributor_resolves_link_content_type(self):
        author = Author(
            name="Author",
            sort_name="Author,",
            link=Link(
                href="http://author",
                rel="contributor",
                type=LinkContentType.OPDS_FEED,
            ),
        )
        serializer = OPDS2Serializer()
        result = serializer._dump_model(serializer._serialize_contributor(author))
        assert result["links"] == [
            {
                "href": "http://author",
                "rel": "contributor",
                "type": opds2.Feed.content_type(),
            }
        ]

    def test_serialize_opds_message(self):
        assert OPDS2Serializer().serialize_opds_message(
            OPDSMessage("URN", 200, "Description")
        ) == dict(urn="URN", description="Description")

    def test_serialize_feed_sort_and_facet_links(self):
        feed_data = FeedData(metadata=FeedMetadata(title="Sort Feed", id="http://feed"))
        feed_data.links.append(Link(href="http://feed", rel="self"))

        # specify a sort link
        link = Link(
            href="test",
            rel="test_rel",
            title="text1",
            facet_group="Sort by",
            active_facet=True,
            default_facet=True,
        )

        # include a non-sort facet
        link2 = Link(
            href="test2",
            title="text2",
            rel="test_2_rel",
            facet_group="test_group",
            active_facet=True,
            default_facet=True,
        )
        link3 = Link(
            href="test3",
            title="text3",
            rel="test_3_rel",
            facet_group="test_group",
        )

        feed_data.facet_links.append(link)
        feed_data.facet_links.append(link2)
        feed_data.facet_links.append(link3)
        links = json.loads(OPDS2Serializer().serialize_feed(feed=feed_data))

        assert links == {
            "publications": [],
            "metadata": {"title": "Sort Feed"},
            "links": [
                {
                    "href": "http://feed",
                    "rel": "self",
                    "type": OPDS2_CONTENT_TYPE,
                },
                {
                    "href": "test",
                    "rel": PALACE_REL_SORT,
                    "title": "text1",
                    "type": OPDS2_CONTENT_TYPE,
                    "properties": {
                        PALACE_PROPERTIES_ACTIVE_SORT: True,
                        PALACE_PROPERTIES_DEFAULT: True,
                    },
                },
            ],
            "facets": [
                {
                    "metadata": {"title": "test_group"},
                    "links": [
                        {
                            "href": "test2",
                            "rel": "self",
                            "title": "text2",
                            "properties": {
                                PALACE_PROPERTIES_DEFAULT: True,
                            },
                        },
                        {
                            "href": "test3",
                            "rel": "test_3_rel",
                            "title": "text3",
                        },
                    ],
                }
            ],
        }

    def test_serialize_feed_skips_entry_without_computed(self):
        """Entries with computed=None are skipped with a warning."""
        feed = FeedData(
            metadata=FeedMetadata(title="Feed", id="http://feed"),
        )
        entry_no_computed = WorkEntry(
            work=Work(),
            edition=Edition(),
            identifier=Identifier(),
        )
        # computed is None by default
        feed.entries = [entry_no_computed]
        feed.links = [Link(href="http://feed", rel="self")]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)
        assert result["publications"] == []

    def test_serialize_feed_skips_invalid_publication(self):
        """A ValidationError during publication serialization is caught and the entry is skipped."""
        feed = FeedData(
            metadata=FeedMetadata(title="Feed", id="http://feed"),
        )
        entry = WorkEntry(
            work=Work(),
            edition=Edition(),
            identifier=Identifier(),
        )
        entry.computed = WorkEntryData(
            identifier="urn:bad",
            title="Bad Work",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
        )
        feed.entries = [entry]
        feed.links = [Link(href="http://feed", rel="self")]

        serializer = OPDS2Serializer()

        # Force a ValidationError when _publication is called
        with patch.object(
            serializer,
            "_publication",
            side_effect=__import__("pydantic").ValidationError.from_exception_data(
                title="Publication",
                line_errors=[],
            ),
        ):
            serialized = serializer.serialize_feed(feed)
        result = json.loads(serialized)
        assert result["publications"] == []

    def test_serialize_metadata_no_title(self):
        """When the feed has no title, it defaults to 'Feed'."""
        feed = FeedData(
            metadata=FeedMetadata(title="", id="http://feed"),
        )
        feed.links = [Link(href="http://feed", rel="self")]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)
        assert result["metadata"]["title"] == "Feed"

    def test_publication_links_skip_without_rel(self):
        """Other links without a rel attribute are skipped."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="Test",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            other_links=[
                Link(href="http://no-rel", rel=None, type="text/html"),
            ],
        )
        entry = serializer.serialize_work_entry(data)
        # The other_link without rel should be skipped; only the acquisition link remains
        link_hrefs = [link["href"] for link in entry["links"]]
        assert "http://no-rel" not in link_hrefs
        assert "http://acq" in link_hrefs

    def test_publication_links_skip_without_type(self):
        """Other links without a type attribute are skipped."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="Test",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            other_links=[
                Link(href="http://no-type", rel="related", type=None),
            ],
        )
        entry = serializer.serialize_work_entry(data)
        link_hrefs = [link["href"] for link in entry["links"]]
        assert "http://no-type" not in link_hrefs

    def test_acquisition_link_type_returns_none(self):
        """An acquisition link with no type and no indirect types returns None."""
        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://no-type",
            rel="acquisition",
            type=None,
            indirect_acquisitions=[],
        )
        result = serializer._serialize_acquisition_link(acquisition)
        assert result is None

    def test_acquisition_link_type_fallback_to_indirect(self):
        """When an acquisition link has no direct type, falls back to the first indirect type."""
        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://indirect-type",
            rel="acquisition",
            type=None,
            indirect_acquisitions=[
                IndirectAcquisition(type="application/epub+zip"),
            ],
        )
        result = serializer._serialize_acquisition_link(acquisition)
        assert result is not None
        dumped = serializer._dump_model(result)
        assert dumped["type"] == "application/epub+zip"

    def test_acquisition_link_type_fallback_to_semantic_indirect(self):
        """Semantic indirect types are resolved when direct type is missing."""
        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://indirect-type",
            rel="acquisition",
            type=None,
            indirect_acquisitions=[
                IndirectAcquisition(type=LinkContentType.OPDS_ENTRY),
            ],
        )
        result = serializer._serialize_acquisition_link(acquisition)
        assert result is not None
        dumped = serializer._dump_model(result)
        assert dumped["type"] == opds2.BasePublication.content_type()

    def test_indirect_acquisition_without_type(self):
        """An indirect acquisition with type=None is skipped."""
        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://acq",
            rel="acquisition",
            type="application/epub+zip",
            indirect_acquisitions=[
                IndirectAcquisition(type=None),
            ],
        )
        result = serializer._dump_model(
            serializer._serialize_acquisition_link(acquisition)
        )
        # The indirect acquisition without type should be filtered out,
        # and the empty list is dropped from serialization
        assert "indirectAcquisition" not in result.get("properties", {})

    def test_feed_link_without_rel(self):
        """Feed links without a rel attribute are skipped."""
        feed = FeedData(
            metadata=FeedMetadata(title="Feed", id="http://feed"),
        )
        feed.links = [
            Link(href="http://feed", rel="self"),
            Link(href="http://no-rel", rel=None),
        ]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)
        link_hrefs = [link["href"] for link in result["links"]]
        assert "http://feed" in link_hrefs
        assert "http://no-rel" not in link_hrefs

    def test_facet_group_with_single_link_skipped(self):
        """A facet group with fewer than 2 links is skipped."""
        feed = FeedData(
            metadata=FeedMetadata(title="Feed", id="http://feed"),
        )
        feed.links = [Link(href="http://feed", rel="self")]
        feed.facet_links = [
            Link(
                href="http://lone-facet",
                rel="facet-rel",
                title="Only One",
                facet_group="LoneGroup",
            ),
        ]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)
        assert "facets" not in result or result.get("facets") == []

    def test_serialize_navigation(self):
        """Navigation entries are serialized from data_entries with NAVIGATION type."""
        feed = FeedData(
            metadata=FeedMetadata(title="Nav Feed", id="http://feed"),
        )
        feed.links = [Link(href="http://feed", rel="self")]
        feed.data_entries = [
            DataEntry(
                type=DataEntryTypes.NAVIGATION,
                title="Nav Entry",
                links=[
                    Link(href="http://nav-link", rel="subsection", type="text/html"),
                ],
            ),
            DataEntry(
                type=DataEntryTypes.NAVIGATION,
                title=None,
                links=[
                    Link(
                        href="http://nav-fallback",
                        rel="subsection",
                        type="text/html",
                        title="Link Title",
                    ),
                ],
            ),
            DataEntry(
                type=DataEntryTypes.NAVIGATION,
                title=None,
                links=[
                    Link(
                        href="http://nav-href-fallback",
                        rel="subsection",
                        type="text/html",
                        title=None,
                    ),
                ],
            ),
        ]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)

        assert "navigation" in result
        nav = result["navigation"]
        assert len(nav) == 3
        assert nav[0]["title"] == "Nav Entry"
        assert nav[0]["href"] == "http://nav-link"
        # Falls back to link.title when entry.title is None
        assert nav[1]["title"] == "Link Title"
        # Falls back to link.href when both titles are None
        assert nav[2]["title"] == "http://nav-href-fallback"

    def test_availability_unknown_status(self):
        """An unknown availability status logs a warning and returns None."""
        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://unknown",
            rel="acquisition",
            type="application/epub+zip",
            availability_status="bogus_status",
        )
        result = serializer._dump_model(
            serializer._serialize_acquisition_link(acquisition)
        )
        # Unknown status should be treated as if no status was provided
        assert "availability" not in result.get("properties", {})

    def test_parse_int_non_numeric(self):
        """Non-numeric strings return None from _parse_int."""
        serializer = OPDS2Serializer()
        assert serializer._parse_int("not-a-number") is None
        assert serializer._parse_int("3.14") is None
        assert serializer._parse_int(None) is None
        assert serializer._parse_int("42") == 42

    def test_serialize_work_entry_no_authors(self):
        """When no authors are provided, author field is omitted from metadata."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="No Authors",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            authors=[],
        )
        entry = serializer.serialize_work_entry(data)
        assert "author" not in entry["metadata"]

    def test_serialize_work_entry_single_author(self):
        """A single author is serialized as an object (not a list)."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="One Author",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            authors=[Author(name="Solo Author")],
        )
        entry = serializer.serialize_work_entry(data)
        assert entry["metadata"]["author"] == {"name": "Solo Author"}

    def test_serialize_work_entry_no_summary_no_publisher_no_imprint(self):
        """Metadata fields are omitted when summary, publisher, and imprint are absent."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="Minimal",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            summary=None,
            publisher=None,
            imprint=None,
        )
        entry = serializer.serialize_work_entry(data)
        metadata = entry["metadata"]
        assert "description" not in metadata
        assert "publisher" not in metadata
        assert "imprint" not in metadata

    def test_serialize_work_entry_default_medium(self):
        """When medium is None, defaults to schema_org Book type."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="Default Type",
            identifier="urn:id",
            medium=None,
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
        )
        entry = serializer.serialize_work_entry(data)
        assert entry["metadata"]["@type"] == "http://schema.org/Book"

    def test_acquisition_link_skipped_returns_none_in_publication_links(self):
        """When _serialize_acquisition_link returns None, no link is appended."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="Test",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                # This acquisition has no type and no indirect type, so it returns None
                Acquisition(
                    href="http://bad-acq",
                    rel="acquisition",
                    type=None,
                    indirect_acquisitions=[],
                ),
                Acquisition(
                    href="http://good-acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                ),
            ],
        )
        entry = serializer.serialize_work_entry(data)
        link_hrefs = [link["href"] for link in entry["links"]]
        assert "http://bad-acq" not in link_hrefs
        assert "http://good-acq" in link_hrefs

    def test_facet_link_title_fallback(self):
        """Facet links fall back to rel or href for title when title is None."""
        feed = FeedData(
            metadata=FeedMetadata(title="Feed", id="http://feed"),
        )
        feed.links = [Link(href="http://feed", rel="self")]
        feed.facet_links = [
            Link(
                href="http://facet1",
                rel="facet-rel",
                title=None,
                facet_group="Group",
            ),
            Link(
                href="http://facet2",
                rel=None,
                title=None,
                facet_group="Group",
            ),
        ]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)
        facet_links = result["facets"][0]["links"]
        # First facet falls back to rel
        assert facet_links[0]["title"] == "facet-rel"
        # Second facet falls back to href
        assert facet_links[1]["title"] == "http://facet2"

    def test_holds_and_copies_with_values(self):
        """Holds and copies numeric values are serialized correctly."""
        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://acq",
            rel="acquisition",
            type="application/epub+zip",
            holds_total="10",
            holds_position="3",
            copies_total="5",
            copies_available="2",
        )
        result = serializer._dump_model(
            serializer._serialize_acquisition_link(acquisition)
        )
        props = result["properties"]
        assert props["holds"] == {"total": 10, "position": 3}
        assert props["copies"] == {"total": 5, "available": 2}

    def test_generic_contributor(self):
        """Contributors with unrecognized MARC roles become generic contributors."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="Generic Contributor Test",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            contributors=[
                Author(name="Some Person", sort_name="Person, Some", role="zzz"),
            ],
        )
        entry = serializer.serialize_work_entry(data)
        metadata = entry["metadata"]
        assert metadata["contributor"] == [
            {"name": "Some Person", "sortAs": "Person, Some", "role": "zzz"}
        ]

    def test_navigation_skips_non_navigation_data_entries(self):
        """Data entries without NAVIGATION type are ignored."""
        feed = FeedData(
            metadata=FeedMetadata(title="Nav Feed", id="http://feed"),
        )
        feed.links = [Link(href="http://feed", rel="self")]
        feed.data_entries = [
            DataEntry(
                type=None,
                title="Not Navigation",
                links=[Link(href="http://ignore", rel="something")],
            ),
            DataEntry(
                type=DataEntryTypes.NAVIGATION,
                title="Real Nav",
                links=[Link(href="http://nav", rel="subsection")],
            ),
        ]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)
        nav = result["navigation"]
        assert len(nav) == 1
        assert nav[0]["title"] == "Real Nav"
        assert nav[0]["href"] == "http://nav"

    def test_resolve_type_maps_opds_feed(self):
        """LinkContentType.OPDS_FEED maps to OPDS2 feed content type."""
        serializer = OPDS2Serializer()
        assert (
            serializer._resolve_type(LinkContentType.OPDS_FEED)
            == opds2.Feed.content_type()
        )

    def test_resolve_type_maps_opds_entry(self):
        """LinkContentType.OPDS_ENTRY maps to OPDS2 publication content type."""
        serializer = OPDS2Serializer()
        assert (
            serializer._resolve_type(LinkContentType.OPDS_ENTRY)
            == opds2.BasePublication.content_type()
        )

    def test_resolve_type_passes_through_concrete_types(self):
        """Concrete content types are passed through unchanged."""
        serializer = OPDS2Serializer()
        assert serializer._resolve_type("text/html") == "text/html"
        assert serializer._resolve_type(None) is None

    def test_feed_link_resolves_link_content_type(self):
        """Feed links with LinkContentType are resolved to OPDS2 types."""
        serializer = OPDS2Serializer()
        link = Link(
            href="http://example.com/shelf",
            rel="http://opds-spec.org/shelf",
            type=LinkContentType.OPDS_FEED,
        )
        result = serializer._serialize_feed_link(link)
        assert result is not None
        assert result.type == opds2.Feed.content_type()

    def test_acquisition_link_resolves_link_content_type(self):
        """Acquisition links with LinkContentType.OPDS_ENTRY are resolved."""
        serializer = OPDS2Serializer()
        link = Acquisition(
            href="http://example.com/borrow",
            rel="http://opds-spec.org/acquisition/borrow",
            type=LinkContentType.OPDS_ENTRY,
        )
        result = serializer._acquisition_link_type(link)
        assert result == opds2.BasePublication.content_type()

    def test_publication_links_resolve_link_content_type(self):
        """Publication other_links with LinkContentType are resolved."""
        serializer = OPDS2Serializer()
        data = WorkEntryData(
            title="Test",
            identifier="urn:id",
            image_links=[Link(href="http://image", rel="image", type="image/png")],
            acquisition_links=[
                Acquisition(
                    href="http://acq",
                    rel=OPDSFeed.OPEN_ACCESS_REL,
                    type="application/epub+zip",
                )
            ],
            other_links=[
                Link(
                    href="http://example.com/recommendations",
                    rel="recommendations",
                    type=LinkContentType.OPDS_FEED,
                    title="Recommended Works",
                ),
            ],
        )
        publication = serializer._publication(data)
        # Find the recommendations link in the publication links
        rec_links = [
            link for link in publication.links if link.rel == "recommendations"
        ]
        assert len(rec_links) == 1
        assert rec_links[0].type == opds2.Feed.content_type()

    def test_profile_link_keeps_standard_rel(self):
        """Profile link with standard 'profile' rel is kept as-is in OPDS2."""
        serializer = OPDS2Serializer()
        link = Link(
            href="http://example.com/profile",
            rel="profile",
        )
        result = serializer._serialize_feed_link(link)
        assert result is not None
        assert result.rel == "profile"
