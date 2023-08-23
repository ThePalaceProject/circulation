import json

from core.feed_protocol.serializer.opds2 import OPDS2Serializer
from core.feed_protocol.types import (
    Acquisition,
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
        feed.entries = [
            WorkEntry(
                work=Work(),
                edition=Edition(),
                identifier=Identifier(),
                computed=WorkEntryData(identifier="identifier", pwid="permanent-id"),
            )
        ]
        feed.links = [Link(href="http://link", rel="link-rel")]
        feed.facet_links = [
            Link(href="http://facet-link", rel="facet-rel", facetGroup="FacetGroup")
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

    def test__serialize_work_entry(self):
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
                FeedEntryType(scheme="scheme", label="label"),
            ],
            series=FeedEntryType(name="Series", position="3"),
            image_links=[Link(href="http://image", rel="image-rel")],
            acquisition_links=[
                Acquisition(href="http://acquisition", rel="acquisition-rel")
            ],
            other_links=[Link(href="http://link", rel="rel")],
        )

        serializer = OPDS2Serializer()

        entry = serializer._serialize_work_entry(data)
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
                "state": "ready",
            },
            indirectAcquisition=[
                {
                    "type": "indirect1",
                    "child": [{"type": "indirect1-1"}, {"type": "indirect1-2"}],
                }
            ],
        )
