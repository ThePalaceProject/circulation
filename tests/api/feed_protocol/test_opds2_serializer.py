import json

from core.feed_protocol.serializer.opds2 import OPDS2Serializer
from core.feed_protocol.types import (
    Acquisition,
    Author,
    FeedData,
    FeedEntryType,
    IndirectAcquisition,
    Link,
    WorkEntry,
    WorkEntryData,
)
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.work import Work


class TestOPDS2Serializer:
    def test_serialize_feed(self):
        feed = FeedData(
            metadata=dict(
                items_per_page=FeedEntryType(text="20"),
                title=FeedEntryType(text="Title"),
            )
        )
        w = WorkEntry(
            work=Work(),
            edition=Edition(),
            identifier=Identifier(),
        )
        w.computed = WorkEntryData(identifier="identifier", pwid="permanent-id")
        feed.entries = [w]
        feed.links = [Link(href="http://link", rel="link-rel")]
        feed.facet_links = [
            Link.create(
                href="http://facet-link", rel="facet-rel", facetGroup="FacetGroup"
            )
        ]

        serialized = OPDS2Serializer().serialize_feed(feed)
        result = json.loads(serialized)

        assert result["metadata"]["title"] == "Title"
        assert result["metadata"]["itemsPerPage"] == 20

        assert len(result["publications"]) == 1
        assert result["publications"][0] == dict(
            metadata={"identifier": "identifier"}, images=[], links=[]
        )

        assert len(result["links"]) == 1
        assert result["links"][0] == dict(href="http://link", rel="link-rel")

        assert len(result["facets"]) == 1
        assert result["facets"][0] == dict(
            metadata={"title": "FacetGroup"},
            links=[{"href": "http://facet-link", "rel": "facet-rel"}],
        )

    def test_serialize_work_entry(self):
        data = WorkEntryData(
            additionalType="type",
            title=FeedEntryType(text="The Title"),
            sort_title=FeedEntryType(text="Title, The"),
            subtitle=FeedEntryType(text="Sub Title"),
            identifier="urn:id",
            language=FeedEntryType(text="de"),
            updated=FeedEntryType(text="2022-02-02"),
            published=FeedEntryType(text="2020-02-02"),
            summary=FeedEntryType(text="Summary"),
            publisher=FeedEntryType(text="Publisher"),
            imprint=FeedEntryType(text="Imprint"),
            categories=[
                FeedEntryType.create(scheme="scheme", label="label"),
            ],
            series=FeedEntryType.create(name="Series", position="3"),
            image_links=[Link(href="http://image", rel="image-rel")],
            acquisition_links=[
                Acquisition(href="http://acquisition", rel="acquisition-rel")
            ],
            other_links=[Link(href="http://link", rel="rel")],
        )

        serializer = OPDS2Serializer()

        entry = serializer.serialize_work_entry(data)
        metadata = entry["metadata"]

        assert metadata["@type"] == data.additionalType
        assert metadata["title"] == data.title.text
        assert metadata["sortAs"] == data.sort_title.text
        assert metadata["subtitle"] == data.subtitle.text
        assert metadata["identifier"] == data.identifier
        assert metadata["language"] == data.language.text
        assert metadata["modified"] == data.updated.text
        assert metadata["published"] == data.published.text
        assert metadata["description"] == data.summary.text
        assert metadata["publisher"] == dict(name=data.publisher.text)
        assert metadata["imprint"] == dict(name=data.imprint.text)
        assert metadata["subject"] == [
            dict(scheme="scheme", name="label", sortAs="label")
        ]
        assert metadata["belongsTo"] == dict(name="Series", position=3)

        assert entry["links"] == [
            dict(href="http://link", rel="rel"),
            dict(href="http://acquisition", rel="acquisition-rel"),
        ]
        assert entry["images"] == [dict(href="http://image", rel="image-rel")]

        # Test the different author types
        data = WorkEntryData(
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
        # Only the first author is considered
        assert metadata["author"] == dict(name="author1")
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
        serializer = OPDS2Serializer()
        acquisition = Acquisition(
            href="http://acquisition",
            rel="acquisition",
            availability_status="available",
            availability_since="2022-02-02",
            availability_until="2222-02-02",
            indirect_acquisitions=[
                IndirectAcquisition(
                    type="indirect1",
                    children=[
                        IndirectAcquisition(type="indirect1-1"),
                        IndirectAcquisition(type="indirect1-2"),
                    ],
                ),
            ],
        )

        result = serializer._serialize_acquisition_link(acquisition)

        assert result["href"] == acquisition.href
        assert result["rel"] == acquisition.rel
        assert result["properties"] == dict(
            availability={
                "since": "2022-02-02",
                "until": "2222-02-02",
                "state": "available",
            },
            indirectAcquisition=[
                {
                    "type": "indirect1",
                    "child": [{"type": "indirect1-1"}, {"type": "indirect1-2"}],
                }
            ],
        )

        # Test availability states
        acquisition = Acquisition(
            href="http://hold",
            rel="hold",
            is_hold=True,
            availability_status="available",
        )
        result = serializer._serialize_acquisition_link(acquisition)
        assert result["properties"]["availability"]["state"] == "reserved"

        acquisition = Acquisition(
            href="http://loan",
            rel="loan",
            is_loan=True,
            availability_status="available",
        )
        result = serializer._serialize_acquisition_link(acquisition)
        assert result["properties"]["availability"]["state"] == "ready"

    def test__serialize_contributor(self):
        author = Author(
            name="Author",
            sort_name="Author,",
            link=Link(href="http://author", rel="contributor", title="Delete me!"),
        )
        result = OPDS2Serializer()._serialize_contributor(author)
        assert result["name"] == "Author"
        assert result["sortAs"] == "Author,"
        assert result["links"] == [{"href": "http://author", "rel": "contributor"}]
