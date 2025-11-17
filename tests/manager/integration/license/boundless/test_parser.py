from __future__ import annotations

import datetime

from palace.manager.integration.license.boundless.model.xml import AvailabilityResponse
from palace.manager.integration.license.boundless.parser import (
    BibliographicParser,
)
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from tests.fixtures.files import BoundlessFilesFixture


class TestBibliographicParser:
    def test_bibliographic_parser(self, boundless_files_fixture: BoundlessFilesFixture):
        # Make sure the bibliographic information gets properly
        # collated in preparation for creating Edition objects.

        data = boundless_files_fixture.sample_data("tiny_collection.xml")
        [bib1, av1], [bib2, av2] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )

        # We test for availability information in a separate test.
        # Here we just make sure it is present.
        assert av1 is not None
        assert av2 is not None

        # But we did get bibliographic information.
        assert bib1 is not None
        assert bib2 is not None

        assert bib1.title == "Faith of My Fathers : A Family Memoir"
        assert bib1.language == "eng"
        assert bib1.published == datetime.date(2000, 3, 7)

        assert bib2.publisher == "Simon & Schuster"
        assert bib2.imprint == "Pocket Books"

        assert bib1.medium == Edition.BOOK_MEDIUM

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        assert bib2.series is None

        # Book #1 has two links -- a description and a cover image.
        [description, cover] = bib1.links
        assert description.rel == Hyperlink.DESCRIPTION
        assert description.media_type == Representation.TEXT_PLAIN
        assert isinstance(description.content, str)
        assert description.content.startswith("John McCain's deeply moving memoir")

        # The cover image simulates the current state of the B&T cover
        # service, where we get a thumbnail-sized image URL in the
        # API response and we can hack the URL to get the
        # full-sized image URL.
        assert cover.rel == LinkRelations.IMAGE
        assert (
            cover.href
            == "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Large/Empty"
        )
        assert cover.media_type == MediaTypes.JPEG_MEDIA_TYPE

        assert cover.thumbnail.rel == LinkRelations.THUMBNAIL_IMAGE
        assert (
            cover.thumbnail.href
            == "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Medium/Empty"
        )
        assert cover.thumbnail.media_type == MediaTypes.JPEG_MEDIA_TYPE

        # Book #1 has a primary author, another author and a narrator.
        [cont1, cont2, narrator] = bib1.contributors
        assert cont1.sort_name == "McCain, John"
        assert cont1.roles == (Contributor.Role.PRIMARY_AUTHOR,)

        assert cont2.sort_name == "Salter, Mark"
        assert cont2.roles == (Contributor.Role.AUTHOR,)

        assert narrator.sort_name == "McCain, John S. III"
        assert narrator.roles == (Contributor.Role.NARRATOR,)

        # Book #2 only has a primary author.
        [cont] = bib2.contributors
        assert cont.sort_name == "Pollero, Rhonda"
        assert cont.roles == (Contributor.Role.PRIMARY_AUTHOR,)

        boundless_id, isbn = sorted(bib1.identifiers, key=lambda x: x.identifier)
        assert boundless_id.identifier == "0003642860"
        assert isbn.identifier == "9780375504587"

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2.subjects, key=lambda x: x.identifier or "")
        assert [x.type for x in subjects] == [
            Subject.BISAC,
            Subject.BISAC,
            Subject.BISAC,
            Subject.AXIS_360_AUDIENCE,
        ]
        general_fiction, women_sleuths, romantic_suspense = sorted(
            x.name for x in subjects if x.type == Subject.BISAC and x.name is not None
        )
        assert general_fiction == "FICTION / General"
        assert women_sleuths == "FICTION / Mystery & Detective / Women Sleuths"
        assert romantic_suspense == "FICTION / Romance / Suspense"

        [adult] = [
            x.identifier for x in subjects if x.type == Subject.AXIS_360_AUDIENCE
        ]
        assert adult == "General Adult"

        # The second book has a cover image simulating some possible
        # future case, where B&T change their cover service so that
        # the size URL hack no longer works. In this case, we treat
        # the image URL as both the full-sized image and the
        # thumbnail.
        [cover] = bib2.links
        assert cover.rel == LinkRelations.IMAGE
        assert cover.href == "http://some-other-server/image.jpg"
        assert cover.media_type == MediaTypes.JPEG_MEDIA_TYPE

        assert cover.thumbnail.rel == LinkRelations.THUMBNAIL_IMAGE
        assert cover.thumbnail.href == "http://some-other-server/image.jpg"
        assert cover.thumbnail.media_type == MediaTypes.JPEG_MEDIA_TYPE

        # The first book is available in two formats -- "ePub" and "AxisNow"
        [adobe, axisnow] = bib1.circulation.formats
        assert adobe.content_type == Representation.EPUB_MEDIA_TYPE
        assert adobe.drm_scheme == DeliveryMechanism.ADOBE_DRM

        assert axisnow.content_type == Representation.EPUB_MEDIA_TYPE
        assert axisnow.drm_scheme == DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM

        # The second book is available in 'Blio' format, which
        # is treated as an alternate name for 'AxisNow'
        [axisnow] = bib2.circulation.formats
        assert axisnow.content_type == Representation.EPUB_MEDIA_TYPE
        assert axisnow.drm_scheme == DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM

    def test_bibliographic_parser_audiobook(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        # TODO - we need a real example to test from. The example we were
        # given is a hacked-up ebook. Ideally we would be able to check
        # narrator information here.
        data = boundless_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )

        [[bib, av]] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )
        assert av is not None
        assert bib is not None

        assert bib.title == "Back Spin"
        assert bib.medium == Edition.AUDIO_MEDIUM

        # The audiobook has one DeliveryMechanism, in which the Findaway licensing document
        # acts as both the content type and the DRM scheme.
        [findaway] = bib.circulation.formats
        assert findaway.content_type is None
        assert findaway.drm_scheme == DeliveryMechanism.FINDAWAY_DRM

        # Although the audiobook is also available in the "AxisNow"
        # format, no second delivery mechanism was created for it, the
        # way it would have been for an ebook.
        assert b"<formatName>AxisNow</formatName>" in data

    def test_bibliographic_parser_blio_format(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        # This book is available as 'Blio' but not 'AxisNow'.
        data = boundless_files_fixture.sample_data(
            "availability_without_fulfillment.xml"
        )
        data = data.replace(b"ePub", b"No Such Format")

        [[bib, av]] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )
        assert av is not None
        assert bib is not None

        # A book in Blio format is treated as an AxisNow ebook.
        assert bib.medium == Edition.BOOK_MEDIUM
        [axisnow] = bib.circulation.formats
        assert axisnow.content_type == Representation.EPUB_MEDIA_TYPE
        assert axisnow.drm_scheme == DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM

    def test_bibliographic_parser_blio_and_axisnow_format(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        # This book is available as both 'Blio' and 'AxisNow'.
        data = boundless_files_fixture.sample_data(
            "availability_with_ebook_fulfillment.xml"
        )

        [[bib, av]] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )
        assert av is not None
        assert bib is not None

        # There is only one FormatData for 'Blio' and 'AxisNow', since they mean the same thing.
        assert bib.medium == Edition.BOOK_MEDIUM
        [adobe, axisnow] = bib.circulation.formats

        assert adobe.content_type == Representation.EPUB_MEDIA_TYPE
        assert adobe.drm_scheme == DeliveryMechanism.ADOBE_DRM

        assert axisnow.content_type == Representation.EPUB_MEDIA_TYPE
        assert axisnow.drm_scheme == DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM

    def test_bibliographic_parser_unsupported_format(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        data = boundless_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        data = data.replace(b"Acoustik", b"No Such Format 1")
        data = data.replace(b"AxisNow", b"No Such Format 2")

        [[bib, av]] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )
        assert av is not None
        assert bib is not None

        # We don't support any of the formats, so no FormatData objects were created.
        assert bib.circulation.formats == []

    def test_parse_author_role(self, boundless_files_fixture: BoundlessFilesFixture):
        """Suffixes on author names are turned into roles."""
        author = "Dyssegaard, Elisabeth Kallick (TRN)"
        parse = BibliographicParser._parse_contributor
        c = parse(author)
        assert c.sort_name == "Dyssegaard, Elisabeth Kallick"
        assert c.roles == (Contributor.Role.TRANSLATOR,)

        # A corporate author is given a normal author role.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False)
        assert c.sort_name == "Bob, Inc."
        assert c.roles == (Contributor.Role.PRIMARY_AUTHOR,)

        c = parse(author, primary_author_found=True)
        assert c.sort_name == "Bob, Inc."
        assert c.roles == (Contributor.Role.AUTHOR,)

        # An unknown author type is given an unknown role
        author = "Eve, Mallory (ZZZ)"
        c = parse(author, primary_author_found=False)
        assert c.sort_name == "Eve, Mallory"
        assert c.roles == (Contributor.Role.UNKNOWN,)

        # force_role overwrites whatever other role might be
        # assigned.
        author = "Bob, Inc. (COR)"
        c = parse(
            author, primary_author_found=False, force_role=Contributor.Role.NARRATOR
        )
        assert c.roles == (Contributor.Role.NARRATOR,)

    def test_availability_parser(self, boundless_files_fixture: BoundlessFilesFixture):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = boundless_files_fixture.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )

        # We already tested the bibliographic information, so we just make sure
        # it is present.
        assert bib1 is not None
        assert bib2 is not None

        # But we did get availability information.
        assert av1 is not None
        assert av2 is not None

        assert av1.primary_identifier_data.identifier == "0003642860"
        assert av1.licenses_owned == 9
        assert av1.licenses_available == 9
        assert av1.patrons_in_hold_queue == 0
        assert av1.status == LicensePoolStatus.ACTIVE

        # Second book also has licenses, so it should be ACTIVE as well
        assert av2.primary_identifier_data.identifier == "0012164897"
        assert av2.licenses_owned == 10
        assert av2.licenses_available == 10
        assert av2.patrons_in_hold_queue == 0
        assert av2.status == LicensePoolStatus.ACTIVE

    def test_availability_parser_exhausted_status(
        self, boundless_files_fixture: BoundlessFilesFixture
    ):
        """Test that status is set to EXHAUSTED when licenses_owned is 0."""
        data = boundless_files_fixture.sample_data("tiny_collection.xml")

        # Modify the data to set total_copies to 0 for both books
        data = data.replace(
            b"<totalCopies>9</totalCopies>", b"<totalCopies>0</totalCopies>"
        )
        data = data.replace(
            b"<totalCopies>10</totalCopies>", b"<totalCopies>0</totalCopies>"
        )

        [bib1, av1], [bib2, av2] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )

        # When licenses_owned is 0, status should be EXHAUSTED
        assert av1.licenses_owned == 0
        assert av1.status == LicensePoolStatus.EXHAUSTED

        assert av2.licenses_owned == 0
        assert av2.status == LicensePoolStatus.EXHAUSTED
