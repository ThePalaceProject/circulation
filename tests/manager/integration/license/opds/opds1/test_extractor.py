import random
from datetime import date
from functools import partial

from freezegun import freeze_time
from lxml import etree

from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.opds.opds1.extractor import Opds1Extractor
from palace.manager.integration.license.opds.opds1.settings import IdentifierSource
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import RightsStatus
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util import first_or_default
from palace.manager.util.opds_writer import AtomFeed
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDSFilesFixture


class TestOPDS1Extractor:
    def test_publication_bibliographic(
        self, opds_files_fixture: OPDSFilesFixture
    ) -> None:
        data_source_name = "Example Data Source"
        extractor = Opds1Extractor("http://example.com/feed", data_source_name)
        feed = extractor.feed_parse(
            opds_files_fixture.sample_data("content_server_mini.opds")
        )
        p1, p2 = list(extractor.feed_publications(feed))

        identifier1 = IdentifierData.parse_urn("http://www.gutenberg.org/ebooks/10441")
        identifier2 = IdentifierData.parse_urn("http://www.gutenberg.org/ebooks/10557")

        assert extractor.publication_identifier(p1) == identifier1
        assert extractor.publication_identifier(p2) == identifier2

        m1 = extractor.publication_bibliographic(identifier1, p1)
        m2 = extractor.publication_bibliographic(identifier2, p2)

        c1 = m1.circulation
        c2 = m2.circulation

        assert m1.title == "The Green Mouse"
        assert m1.subtitle == "A Tale of Mousy Terror"

        assert m1.data_source_name == data_source_name
        assert m2.data_source_name == data_source_name
        assert c1.data_source_name == "Gutenberg"
        assert c2.data_source_name == data_source_name

        assert c1.should_track_playtime == True
        assert c2.should_track_playtime == False

    def test_use_dcterm_identifier_as_id_with_id_and_dcterms_identifier(
        self, opds_files_fixture: OPDSFilesFixture
    ) -> None:

        data_source_name = "Example Data Source"
        extractor = Opds1Extractor(
            "http://example.com/feed",
            data_source_name,
            IdentifierSource.DCTERMS_IDENTIFIER,
        )

        feed = extractor.feed_parse(
            opds_files_fixture.sample_data("feed_with_id_and_dcterms_identifier.opds")
        )

        book1_pub, book2_pub, book3_pub = extractor.feed_publications(feed)

        # First book doesn't have <dcterms:identifier>, so <id> must be used as identifier
        book1_identifier = IdentifierData.parse_urn("https://root.uri/1")
        assert extractor.publication_identifier(book1_pub) == book1_identifier
        assert (
            extractor.publication_bibliographic(
                book1_identifier, book1_pub
            ).primary_identifier_data
            == book1_identifier
        )

        # Second book has <id> and <dcterms:identifier>, so <dcters:identifier> must be used as id
        book2_identifier = IdentifierData.parse_urn("urn:isbn:9781468316438")
        assert extractor.publication_identifier(book2_pub) == book2_identifier
        book_2 = extractor.publication_bibliographic(book2_identifier, book2_pub)
        assert book_2.primary_identifier_data == book2_identifier
        # Verify that id is in identifiers
        assert IdentifierData.parse_urn("https://root.uri/2") in book_2.identifiers

        # Third book has more than one dcterms:identifers, all of them must be present as bibliographic identifier
        book3_identifier = IdentifierData.parse_urn("urn:isbn:9781683351993")
        assert extractor.publication_identifier(book3_pub) == book3_identifier
        book_3 = extractor.publication_bibliographic(book3_identifier, book3_pub)

        assert book_3.primary_identifier_data == book3_identifier
        assert book_3.identifiers == [
            book3_identifier,
            IdentifierData.parse_urn("urn:isbn:9781683351504"),
            IdentifierData.parse_urn("urn:isbn:9780312939458"),
            IdentifierData.parse_urn("https://root.uri/3"),
        ]

    def test_use_id_with_existing_dcterms_identifier(
        self, opds_files_fixture: OPDSFilesFixture
    ) -> None:

        data_source_name = "Example Data Source"
        extractor = Opds1Extractor(
            "http://example.com/feed", data_source_name, IdentifierSource.ID
        )

        feed = extractor.feed_parse(
            opds_files_fixture.sample_data("feed_with_id_and_dcterms_identifier.opds")
        )

        book1_pub, book2_pub, book3_pub = extractor.feed_publications(feed)
        assert extractor.publication_identifier(book1_pub) == IdentifierData.parse_urn(
            "https://root.uri/1"
        )
        assert extractor.publication_identifier(book2_pub) == IdentifierData.parse_urn(
            "https://root.uri/2"
        )
        assert extractor.publication_identifier(book3_pub) == IdentifierData.parse_urn(
            "https://root.uri/3"
        )

    def test__extract_link(self) -> None:
        extract_link = partial(
            Opds1Extractor._extract_link,
            feed_url="http://server",
            entry_rights_uri=RightsStatus.UNKNOWN,
        )

        no_rel = AtomFeed.E.link(href="http://foo/")
        assert extract_link(no_rel) is None

        no_href = AtomFeed.E.link(href="", rel="foo")
        assert extract_link(no_href) is None

        good = AtomFeed.E.link(href="http://foo", rel="bar")
        link = extract_link(good)
        assert link.href == "http://foo"
        assert link.rel == "bar"

        relative = AtomFeed.E.link(href="/foo/bar", rel="self")
        link = extract_link(relative)
        assert link.href == "http://server/foo/bar"

    def test__extract_link_rights_uri(self) -> None:
        # Most of the time, a link's rights URI is inherited from the entry.
        extract_link = partial(
            Opds1Extractor._extract_link,
            feed_url="http://server",
            entry_rights_uri=RightsStatus.PUBLIC_DOMAIN_USA,
        )

        link_tag = AtomFeed.E.link(href="http://foo", rel="bar")
        link = extract_link(link_tag)
        assert link.rights_uri == RightsStatus.PUBLIC_DOMAIN_USA

        # But a dcterms:rights tag beneath the link can override this.
        rights_attr = "{%s}rights" % AtomFeed.DCTERMS_NS
        link_tag.attrib[rights_attr] = RightsStatus.IN_COPYRIGHT
        link = extract_link(link_tag)
        assert link.rights_uri == RightsStatus.IN_COPYRIGHT

    def test__derive_medium_from_links(self) -> None:
        derive_medium_from_links = Opds1Extractor._derive_medium_from_links

        # Test with audio media types
        for media_type in MediaTypes.AUDIOBOOK_MEDIA_TYPES:
            links = [
                LinkData(
                    href="url",
                    rel="http://opds-spec.org/acquisition/",
                    media_type=media_type + ";param=value",
                ),
                LinkData(href="url", rel="http://opds-spec.org/image"),
            ]
            assert derive_medium_from_links(links) == "Audio"

        # Test with book media types
        for media_type in MediaTypes.BOOK_MEDIA_TYPES:
            links = [
                LinkData(
                    href="url",
                    rel="http://opds-spec.org/acquisition/",
                    media_type=media_type + ";param=value",
                ),
                LinkData(href="url", rel="http://opds-spec.org/image"),
            ]
            assert derive_medium_from_links(links) == "Book"

    def test__extract_medium(self):
        m = partial(Opds1Extractor._extract_medium, default="Default")

        # No tag -- the default is used.
        assert m(etree.fromstring("<entry/>")) == "Default"

        def medium(additional_type: str | None, format: str | None) -> str | None:
            # Make an <atom:entry> tag with the given tags.
            # Parse it and call extract_medium on it.
            entry = '<entry xmlns:schema="http://schema.org/" xmlns:dcterms="http://purl.org/dc/terms/"'
            if additional_type:
                entry += ' schema:additionalType="%s"' % additional_type
            entry += ">"
            if format:
                entry += "<dcterms:format>%s</dcterms:format>" % format
            entry += "</entry>"
            tag = etree.fromstring(entry)
            return m(tag)

        audio_type = random.choice(MediaTypes.AUDIOBOOK_MEDIA_TYPES) + ";param=value"
        ebook_type = random.choice(MediaTypes.BOOK_MEDIA_TYPES) + ";param=value"

        # schema:additionalType is checked first. If present, any
        # potentially contradictory information in dcterms:format is
        # ignored.
        assert (
            medium("http://bib.schema.org/Audiobook", ebook_type)
            == Edition.AUDIO_MEDIUM
        )
        assert medium("http://schema.org/EBook", audio_type) == Edition.BOOK_MEDIUM

        # When schema:additionalType is missing or not useful, the
        # value of dcterms:format is mapped to a medium using
        # Edition.medium_from_media_type.
        assert medium("something-else", audio_type) == Edition.AUDIO_MEDIUM
        assert medium(None, ebook_type) == Edition.BOOK_MEDIUM

        # If both pieces of information are missing or useless, the
        # default is used.
        assert medium(None, None) == "Default"
        assert medium("something-else", "image/jpeg") == "Default"

    def test_extract_feed_data_from_feedparser(
        self, opds_files_fixture: OPDSFilesFixture
    ) -> None:
        data_source_name = "Example Data Source"
        extractor = Opds1Extractor("http://example.com/feed", data_source_name)

        feed = extractor.feed_parse(
            opds_files_fixture.sample_data("content_server_mini.opds")
        )

        publications = list(extractor.feed_publications(feed))
        assert publications

        # The <entry> tag became a bibliographic object.
        identifier = IdentifierData.parse_urn(
            "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"
        )
        bibliographic = extractor.publication_bibliographic(identifier, publications[0])
        assert bibliographic.primary_identifier_data == identifier

        assert bibliographic.title == "The Green Mouse"
        assert bibliographic.subtitle == "A Tale of Mousy Terror"
        assert bibliographic.language == "eng"
        assert bibliographic.publisher == "Project Gutenberg"

        circulation = bibliographic.circulation
        assert circulation.data_source_name == DataSource.GUTENBERG

    @freeze_time()
    def test_extract_feed_data_from_elementtree(
        self, opds_files_fixture: OPDSFilesFixture
    ) -> None:
        data_source_name = "Example Data Source"
        extractor = Opds1Extractor("http://example.com/feed", data_source_name)

        feed = extractor.feed_parse(
            opds_files_fixture.sample_data("content_server.opds")
        )

        publications = list(extractor.feed_publications(feed))
        assert publications

        data = [
            extractor.publication_bibliographic(
                extractor.publication_identifier(pub), pub
            )
            for pub in publications
        ]

        # There are 76 entries in the feed, and we got bibliographic for
        # every one of them.
        assert len(data) == 76

        # We're going to do spot checks on a book and a periodical.

        # First, the book.
        book_id = IdentifierData.parse_urn(
            "urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022"
        )
        book = first_or_default(b for b in data if b.primary_identifier_data == book_id)
        assert book is not None
        assert book.medium == Edition.BOOK_MEDIUM

        [contributor] = book.contributors
        assert contributor.sort_name == "Thoreau, Henry David"
        assert contributor.roles == (Contributor.Role.AUTHOR,)

        assert book.subjects == [
            SubjectData(type="LCSH", identifier="Essays", name=None, weight=1),
            SubjectData(type="LCSH", identifier="Nature", name=None, weight=1),
            SubjectData(type="LCSH", identifier="Walking", name=None, weight=1),
            SubjectData(
                type="LCC", identifier="PS", name="American Literature", weight=10
            ),
        ]

        assert book.measurements == []

        assert book.published == date(1862, 6, 1)

        assert book.links == []
        assert book.circulation.links == [
            LinkData(
                href="http://www.gutenberg.org/ebooks/1022.epub.noimages",
                rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
                media_type=Representation.EPUB_MEDIA_TYPE,
            )
        ]

        # And now, the periodical.
        periodical_id = IdentifierData.parse_urn(
            "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"
        )
        periodical = first_or_default(
            b for b in data if b.primary_identifier_data == periodical_id
        )
        assert periodical is not None
        assert periodical.medium == Edition.PERIODICAL_MEDIUM

        assert periodical.subjects == [
            SubjectData(type="LCSH", identifier="Courtship -- Fiction", weight=1),
            SubjectData(type="LCSH", identifier="New York (N.Y.) -- Fiction", weight=1),
            SubjectData(type="LCSH", identifier="Fantasy fiction", weight=1),
            SubjectData(type="LCSH", identifier="Magic -- Fiction", weight=1),
            SubjectData(type="LCC", identifier="PZ", weight=1),
            SubjectData(type="schema:audience", identifier="Children", weight=1),
            SubjectData(type="schema:typicalAgeRange", identifier="7", weight=1),
        ]

        assert periodical.measurements == [
            MeasurementData(
                quantity_measured=Measurement.QUALITY, value=0.3333, weight=1
            ),
            MeasurementData(quantity_measured=Measurement.RATING, value=0.6, weight=1),
            MeasurementData(
                quantity_measured=Measurement.POPULARITY, value=0.25, weight=1
            ),
        ]

        assert periodical.series == "Animal Colors"
        assert periodical.series_position == 1

        assert periodical.published == date(1910, 1, 1)

    def test__consolidate_xml_links(self, db: DatabaseTransactionFixture) -> None:
        links = [
            LinkData(href=db.fresh_url(), rel=rel, media_type="image/jpeg")
            for rel in [
                Hyperlink.OPEN_ACCESS_DOWNLOAD,
                Hyperlink.IMAGE,
                Hyperlink.THUMBNAIL_IMAGE,
                Hyperlink.OPEN_ACCESS_DOWNLOAD,
            ]
        ]
        thumbnail_link = links[2]
        links = Opds1Extractor._consolidate_xml_links(links)
        assert [x.rel for x in links] == [
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            Hyperlink.IMAGE,
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
        ]
        link = links[1]
        assert link.thumbnail == thumbnail_link

        links = [
            LinkData(href=db.fresh_url(), rel=rel, media_type="image/jpeg")
            for rel in [
                Hyperlink.THUMBNAIL_IMAGE,
                Hyperlink.IMAGE,
                Hyperlink.THUMBNAIL_IMAGE,
                Hyperlink.IMAGE,
            ]
        ]
        t1, i1, t2, i2 = links
        links = Opds1Extractor._consolidate_xml_links(links)
        assert [x.rel for x in links] == [Hyperlink.IMAGE, Hyperlink.IMAGE]
        i1, i2 = links
        assert i1.thumbnail == t1
        assert i2.thumbnail == t2

        links = [
            LinkData(href=db.fresh_url(), rel=rel, media_type="image/jpeg")
            for rel in [Hyperlink.THUMBNAIL_IMAGE, Hyperlink.IMAGE, Hyperlink.IMAGE]
        ]
        t1, i1, i2 = links
        links = Opds1Extractor._consolidate_xml_links(links)
        assert [x.rel for x in links] == [Hyperlink.IMAGE, Hyperlink.IMAGE]
        i1, i2 = links
        assert i1.thumbnail == t1
        assert i2.thumbnail is None
