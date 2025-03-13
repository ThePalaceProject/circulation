from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from palace.manager.api.overdrive.representation import OverdriveRepresentationExtractor
from palace.manager.api.overdrive.util import _make_link_safe
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.metadata_layer import LinkData
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from tests.fixtures.overdrive import OverdriveAPIFixture


class TestOverdriveRepresentationExtractor:
    def test_availability_info(self, overdrive_api_fixture: OverdriveAPIFixture):
        data, raw = overdrive_api_fixture.sample_json("overdrive_book_list.json")
        availability = OverdriveRepresentationExtractor.availability_link_list(raw)
        # Every item in the list has a few important values.
        for item in availability:
            for key in "availability_link", "author_name", "id", "title", "date_added":
                assert key in item

        # Also run a spot check on the actual values.
        spot = availability[0]
        assert "210bdcad-29b7-445f-8d05-cdbb40abc03a" == spot["id"]
        assert "King and Maxwell" == spot["title"]
        assert "David Baldacci" == spot["author_name"]
        assert "2013-11-12T14:13:00-05:00" == spot["date_added"]

    def test_availability_info_missing_data(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # overdrive_book_list_missing_data.json has two products. One
        # only has a title, the other only has an ID.
        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_book_list_missing_data.json"
        )
        [item] = OverdriveRepresentationExtractor.availability_link_list(raw)

        # We got a data structure -- full of missing data -- for the
        # item that has an ID.
        assert "i only have an id" == item["id"]
        assert None == item["title"]
        assert None == item["author_name"]
        assert None == item["date_added"]

        # We did not get a data structure for the item that only has a
        # title, because an ID is required -- otherwise we don't know
        # what book we're talking about.

    def test_link(self, overdrive_api_fixture: OverdriveAPIFixture):
        data, raw = overdrive_api_fixture.sample_json("overdrive_book_list.json")
        expect = _make_link_safe(
            "http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open"
        )
        assert expect == OverdriveRepresentationExtractor.link(raw, "first")

    def test_book_info_to_circulation(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Tests that can convert an overdrive json block into a CirculationData object.
        fixture = overdrive_api_fixture
        session = overdrive_api_fixture.db.session

        raw, info = fixture.sample_json("overdrive_availability_information_2.json")
        extractor = OverdriveRepresentationExtractor(fixture.api)
        circulationdata = extractor.book_info_to_circulation(info)

        # NOTE: It's not realistic for licenses_available and
        # patrons_in_hold_queue to both be nonzero; this is just to
        # verify that the test picks up whatever data is in the
        # document.
        assert 3 == circulationdata.licenses_owned
        assert 1 == circulationdata.licenses_available
        assert 10 == circulationdata.patrons_in_hold_queue

        # Related IDs.
        identifier = circulationdata.primary_identifier(session)
        assert (Identifier.OVERDRIVE_ID, "2a005d55-a417-4053-b90d-7a38ca6d2065") == (
            identifier.type,
            identifier.identifier,
        )

    def test_book_info_to_circulation_advantage(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # Overdrive Advantage accounts (a.k.a. "child" or "sub" accounts derive
        # different information from the same API responses as "main" Overdrive
        # accounts.
        fixture = overdrive_api_fixture
        raw, info = fixture.sample_json("overdrive_availability_advantage.json")

        extractor = OverdriveRepresentationExtractor(fixture.api)
        # Calling in the context of a main account should return a count of
        # the main account and any shared sub account owned and available.
        consortial_data = extractor.book_info_to_circulation(info)
        assert 10 == consortial_data.licenses_owned
        assert 10 == consortial_data.licenses_available

        # Pretend to be an API for an Overdrive Advantage collection with
        # library ID 61.
        extractor = OverdriveRepresentationExtractor(MagicMock(advantage_library_id=61))
        advantage_data = extractor.book_info_to_circulation(info)
        assert 1 == advantage_data.licenses_owned
        assert 1 == advantage_data.licenses_available

        # Both collections have the same information about active
        # holds, because that information is not split out by
        # collection.
        assert 0 == advantage_data.patrons_in_hold_queue
        assert 0 == consortial_data.patrons_in_hold_queue

        # If for whatever reason Overdrive doesn't mention the
        # relevant collection at all, no collection-specific
        # information is gleaned.
        #
        # TODO: It would probably be better not to return a
        # CirculationData object at all, but this shouldn't happen in
        # a real scenario.
        extractor = OverdriveRepresentationExtractor(MagicMock(advantage_library_id=62))
        advantage_data = extractor.book_info_to_circulation(info)
        assert 0 == advantage_data.licenses_owned
        assert 0 == advantage_data.licenses_available

        # Pretend to be an API for an Overdrive Advantage collection with
        # library ID 63 which contains shared copies.
        extractor = OverdriveRepresentationExtractor(MagicMock(advantage_library_id=63))
        advantage_data = extractor.book_info_to_circulation(info)
        # since these copies are shared and counted as part of the main
        # context we do not count them here.
        assert 0 == advantage_data.licenses_owned
        assert 0 == advantage_data.licenses_available

    def test_not_found_error_to_circulationdata(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        fixture = overdrive_api_fixture
        transaction = fixture.db
        raw, info = fixture.sample_json("overdrive_availability_not_found.json")

        # By default, a "NotFound" error can't be converted to a
        # CirculationData object, because we don't know _which_ book it
        # was that wasn't found.
        extractor = OverdriveRepresentationExtractor(fixture.api)
        m = extractor.book_info_to_circulation
        with pytest.raises(
            PalaceValueError, match="Book must have an id to be processed"
        ):
            m(info)

        # However, if an ID was added to `info` ahead of time (as the
        # circulation code does), we do know, and we can create a
        # CirculationData.
        identifier = transaction.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        info["id"] = identifier.identifier
        data = m(info)
        assert identifier == data.primary_identifier(transaction.session)
        assert 0 == data.licenses_owned
        assert 0 == data.licenses_available
        assert 0 == data.patrons_in_hold_queue

    def test_book_info_with_metadata(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Tests that can convert an overdrive json block into a Metadata object.

        raw, info = overdrive_api_fixture.sample_json("overdrive_metadata.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        assert "Agile Documentation" == metadata.title
        assert (
            "Agile Documentation A Pattern Guide to Producing Lightweight Documents for Software Projects"
            == metadata.sort_title
        )
        assert (
            "A Pattern Guide to Producing Lightweight Documents for Software Projects"
            == metadata.subtitle
        )
        assert Edition.BOOK_MEDIUM == metadata.medium
        assert "Wiley Software Patterns" == metadata.series
        assert "eng" == metadata.language
        assert "Wiley" == metadata.publisher
        assert "John Wiley & Sons, Inc." == metadata.imprint
        assert 2005 == metadata.published.year
        assert 1 == metadata.published.month
        assert 31 == metadata.published.day

        [author] = metadata.contributors
        assert "Rüping, Andreas" == author.sort_name
        assert "Andreas R&#252;ping" == author.display_name
        assert [Contributor.Role.AUTHOR] == author.roles

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)

        assert [
            ("Computer Technology", Subject.OVERDRIVE, 100),
            ("Nonfiction", Subject.OVERDRIVE, 100),
            ("Object Technologies - Miscellaneous", "tag", 1),
        ] == [(x.identifier, x.type, x.weight) for x in subjects]

        # Related IDs.
        assert (Identifier.OVERDRIVE_ID, "3896665d-9d81-4cac-bd43-ffc5066de1f5") == (
            metadata.primary_identifier.type,
            metadata.primary_identifier.identifier,
        )

        ids = [(x.type, x.identifier) for x in metadata.identifiers]

        # The original data contains an actual ASIN and ISBN, plus a blank
        # ASIN and three invalid ISBNs: one which is common placeholder
        # text, one which is mis-typed and has a bad check digit, and one
        # which has an invalid character; the bad identifiers do not show
        # up here.
        assert [
            (Identifier.ASIN, "B000VI88N2"),
            (Identifier.ISBN, "9780470856246"),
            (Identifier.OVERDRIVE_ID, "3896665d-9d81-4cac-bd43-ffc5066de1f5"),
        ] == sorted(ids)

        # Available formats.
        [kindle, pdf] = sorted(
            metadata.circulation.formats, key=lambda x: x.content_type
        )
        assert DeliveryMechanism.KINDLE_CONTENT_TYPE == kindle.content_type
        assert DeliveryMechanism.KINDLE_DRM == kindle.drm_scheme

        assert Representation.PDF_MEDIA_TYPE == pdf.content_type
        assert DeliveryMechanism.ADOBE_DRM == pdf.drm_scheme

        # Links to various resources.
        shortd, image, longd = sorted(metadata.links, key=lambda x: x.rel)

        assert Hyperlink.DESCRIPTION == longd.rel
        assert longd.content.startswith("<p>Software documentation")

        assert Hyperlink.SHORT_DESCRIPTION == shortd.rel
        assert shortd.content.startswith("<p>Software documentation")
        assert len(shortd.content) < len(longd.content)

        assert Hyperlink.IMAGE == image.rel
        assert (
            "http://images.contentreserve.com/ImageType-100/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg100.jpg"
            == image.href
        )

        thumbnail = image.thumbnail

        assert Hyperlink.THUMBNAIL_IMAGE == thumbnail.rel
        assert (
            "http://images.contentreserve.com/ImageType-200/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg200.jpg"
            == thumbnail.href
        )

        # Measurements associated with the book.

        measurements = metadata.measurements
        popularity = [
            x for x in measurements if x.quantity_measured == Measurement.POPULARITY
        ][0]
        assert 2 == popularity.value

        rating = [x for x in measurements if x.quantity_measured == Measurement.RATING][
            0
        ]
        assert 1 == rating.value

        # Request only the bibliographic information.
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(
            info, include_bibliographic=True, include_formats=False
        )

        assert "Agile Documentation" == metadata.title
        assert None == metadata.circulation

        # Request only the format information.
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(
            info, include_bibliographic=False, include_formats=True
        )

        assert None == metadata.title

        [kindle, pdf] = sorted(
            metadata.circulation.formats, key=lambda x: x.content_type
        )
        assert DeliveryMechanism.KINDLE_CONTENT_TYPE == kindle.content_type
        assert DeliveryMechanism.KINDLE_DRM == kindle.drm_scheme

        assert Representation.PDF_MEDIA_TYPE == pdf.content_type
        assert DeliveryMechanism.ADOBE_DRM == pdf.drm_scheme

    def test_audiobook_info(self, overdrive_api_fixture: OverdriveAPIFixture):
        # This book will be available in three formats: a link to the
        # Overdrive Read website, a manifest file that SimplyE can
        # download, and the legacy format used by the mobile app
        # called 'Overdrive'.
        raw, info = overdrive_api_fixture.sample_json("audiobook.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        streaming, manifest, legacy = sorted(
            metadata.circulation.formats, key=lambda x: x.content_type
        )
        assert DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE == streaming.content_type
        assert (
            MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE == manifest.content_type
        )
        assert "application/x-od-media" == legacy.content_type
        assert (
            metadata.duration == 10 * 3600 + 9 * 60 + 1
        )  # The last formats' duration attribute

        # The last format will be invalid, so only the first format should work
        info["formats"][1]["duration"] = "10:09"  # Invalid format
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        assert (
            metadata.duration == 10 * 3600 + 9 * 60 + 0
        )  # The first formats' duration attribute

    def test_book_info_with_sample(self, overdrive_api_fixture: OverdriveAPIFixture):
        # This book has two samples; one available as a direct download and
        # one available through a manifest file.
        raw, info = overdrive_api_fixture.sample_json("has_sample.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        samples = [x for x in metadata.links if x.rel == Hyperlink.SAMPLE]
        epub_sample, manifest_sample = sorted(samples, key=lambda x: x.media_type or "")

        # Here's the direct download.
        assert (
            "http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub"
            == epub_sample.href
        )
        assert MediaTypes.EPUB_MEDIA_TYPE == epub_sample.media_type

        # Here's the manifest.
        assert (
            "https://samples.overdrive.com/?crid=9BD24F82-35C0-4E0A-B5E7-BCFED07835CF&.epub-sample.overdrive.com"
            == manifest_sample.href
        )
        # Assert we have the end content type of the sample, no DRM formats
        assert "text/html" == manifest_sample.media_type

    def test_book_info_with_unknown_sample(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        raw, info = overdrive_api_fixture.sample_json("has_sample.json")

        # Just use one format, and change a sample type to unknown
        # Only one (known sample) should be extracted then
        info["formats"] = [info["formats"][1]]
        info["formats"][0]["samples"][1]["formatType"] = "overdrive-unknown"
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        samples = [x for x in metadata.links if x.rel == Hyperlink.SAMPLE]

        assert 1 == len(samples)
        assert samples[0].media_type == MediaTypes.EPUB_MEDIA_TYPE

    def test_book_info_with_grade_levels(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        raw, info = overdrive_api_fixture.sample_json("has_grade_levels.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        grade_levels = sorted(
            x.identifier or "fail"
            for x in metadata.subjects
            if x.type == Subject.GRADE_LEVEL
        )
        assert ["Grade 4", "Grade 5", "Grade 6", "Grade 7", "Grade 8"] == grade_levels

    def test_book_info_with_awards(self, overdrive_api_fixture: OverdriveAPIFixture):
        raw, info = overdrive_api_fixture.sample_json("has_awards.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        [awards] = [
            x
            for x in metadata.measurements
            if Measurement.AWARDS == x.quantity_measured
        ]
        assert 1 == awards.value
        assert 1 == awards.weight

    def test_image_link_to_linkdata(self):
        def m(link):
            return OverdriveRepresentationExtractor.image_link_to_linkdata(link, "rel")

        # Test missing data.
        assert None == m(None)
        assert None == m(dict())

        # Test an ordinary success case.
        url = "http://images.overdrive.com/image.png"
        type = "image/type"
        data = m(dict(href=url, type=type))
        assert isinstance(data, LinkData)
        assert url == data.href
        assert type == data.media_type

        # Test a case where no media type is provided.
        data = m(dict(href=url))
        assert None == data.media_type

        # Verify that invalid URLs are made link-safe.
        data = m(dict(href="http://api.overdrive.com/v1/foo:bar"))
        assert "http://api.overdrive.com/v1/foo%3Abar" == data.href

        # Stand-in cover images are detected and filtered out.
        data = m(
            dict(
                href="https://img1.od-cdn.com/ImageType-100/0293-1/{00000000-0000-0000-0000-000000000002}Img100.jpg"
            )
        )
        assert None == data

    def test_internal_formats(self):
        # Overdrive's internal format names may correspond to one or more
        # delivery mechanisms.
        def assert_formats(overdrive_name, *expect):
            actual = OverdriveRepresentationExtractor.internal_formats(overdrive_name)
            assert list(expect) == list(actual)

        # Most formats correspond to one delivery mechanism.
        assert_formats(
            "ebook-pdf-adobe", (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        )

        assert_formats(
            "ebook-epub-open", (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        )

        # ebook-overdrive and audiobook-overdrive each correspond to
        # two delivery mechanisms.
        assert_formats(
            "ebook-overdrive",
            (
                MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM,
            ),
            (
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
            ),
        )

        assert_formats(
            "audiobook-overdrive",
            (
                MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM,
            ),
            (
                DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
            ),
        )

        # An unrecognized format does not correspond to any delivery
        # mechanisms.
        assert_formats("no-such-format")
