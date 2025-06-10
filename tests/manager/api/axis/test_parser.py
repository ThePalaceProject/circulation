from __future__ import annotations

import datetime

from palace.manager.api.axis.models.xml import AvailabilityResponse
from palace.manager.api.axis.parser import (
    BibliographicParser,
)
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from tests.fixtures.files import AxisFilesFixture


class TestBibliographicParser:
    def test_bibliographic_parser(self, axis_files_fixture: AxisFilesFixture):
        # Make sure the bibliographic information gets properly
        # collated in preparation for creating Edition objects.

        data = axis_files_fixture.sample_data("tiny_collection.xml")
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

        assert "Faith of My Fathers : A Family Memoir" == bib1.title
        assert "eng" == bib1.language
        assert datetime.date(2000, 3, 7) == bib1.published

        assert "Simon & Schuster" == bib2.publisher
        assert "Pocket Books" == bib2.imprint

        assert Edition.BOOK_MEDIUM == bib1.medium

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        assert None == bib2.series

        # Book #1 has two links -- a description and a cover image.
        [description, cover] = bib1.links
        assert Hyperlink.DESCRIPTION == description.rel
        assert Representation.TEXT_PLAIN == description.media_type
        assert isinstance(description.content, str)
        assert description.content.startswith("John McCain's deeply moving memoir")

        # The cover image simulates the current state of the B&T cover
        # service, where we get a thumbnail-sized image URL in the
        # Axis 360 API response and we can hack the URL to get the
        # full-sized image URL.
        assert LinkRelations.IMAGE == cover.rel
        assert (
            "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Large/Empty"
            == cover.href
        )
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.media_type

        assert LinkRelations.THUMBNAIL_IMAGE == cover.thumbnail.rel
        assert (
            "http://contentcafecloud.baker-taylor.com/Jacket.svc/D65D0665-050A-487B-9908-16E6D8FF5C3E/9780375504587/Medium/Empty"
            == cover.thumbnail.href
        )
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.thumbnail.media_type

        # Book #1 has a primary author, another author and a narrator.
        #
        # TODO: The narrator data is simulated. we haven't actually
        # verified that Axis 360 sends narrator information in the
        # same format as author information.
        [cont1, cont2, narrator] = bib1.contributors
        assert "McCain, John" == cont1.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == cont1.roles

        assert "Salter, Mark" == cont2.sort_name
        assert (Contributor.Role.AUTHOR,) == cont2.roles

        assert "McCain, John S. III" == narrator.sort_name
        assert (Contributor.Role.NARRATOR,) == narrator.roles

        # Book #2 only has a primary author.
        [cont] = bib2.contributors
        assert "Pollero, Rhonda" == cont.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == cont.roles

        axis_id, isbn = sorted(bib1.identifiers, key=lambda x: x.identifier)
        assert "0003642860" == axis_id.identifier
        assert "9780375504587" == isbn.identifier

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2.subjects, key=lambda x: x.identifier or "")
        assert [
            Subject.BISAC,
            Subject.BISAC,
            Subject.BISAC,
            Subject.AXIS_360_AUDIENCE,
        ] == [x.type for x in subjects]
        general_fiction, women_sleuths, romantic_suspense = sorted(
            x.name for x in subjects if x.type == Subject.BISAC and x.name is not None
        )
        assert "FICTION / General" == general_fiction
        assert "FICTION / Mystery & Detective / Women Sleuths" == women_sleuths
        assert "FICTION / Romance / Suspense" == romantic_suspense

        [adult] = [
            x.identifier for x in subjects if x.type == Subject.AXIS_360_AUDIENCE
        ]
        assert "General Adult" == adult

        # The second book has a cover image simulating some possible
        # future case, where B&T change their cover service so that
        # the size URL hack no longer works. In this case, we treat
        # the image URL as both the full-sized image and the
        # thumbnail.
        [cover] = bib2.links
        assert LinkRelations.IMAGE == cover.rel
        assert "http://some-other-server/image.jpg" == cover.href
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.media_type

        assert LinkRelations.THUMBNAIL_IMAGE == cover.thumbnail.rel
        assert "http://some-other-server/image.jpg" == cover.thumbnail.href
        assert MediaTypes.JPEG_MEDIA_TYPE == cover.thumbnail.media_type

        # The first book is available in two formats -- "ePub" and "AxisNow"
        [adobe, axisnow] = bib1.circulation.formats
        assert Representation.EPUB_MEDIA_TYPE == adobe.content_type
        assert DeliveryMechanism.ADOBE_DRM == adobe.drm_scheme

        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

        # The second book is available in 'Blio' format, which
        # is treated as an alternate name for 'AxisNow'
        [axisnow] = bib2.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_audiobook(self, axis_files_fixture: AxisFilesFixture):
        # TODO - we need a real example to test from. The example we were
        # given is a hacked-up ebook. Ideally we would be able to check
        # narrator information here.
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )

        [[bib, av]] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )
        assert av is not None
        assert bib is not None

        assert "Back Spin" == bib.title
        assert Edition.AUDIO_MEDIUM == bib.medium

        # The audiobook has one DeliveryMechanism, in which the Findaway licensing document
        # acts as both the content type and the DRM scheme.
        [findaway] = bib.circulation.formats
        assert None == findaway.content_type
        assert DeliveryMechanism.FINDAWAY_DRM == findaway.drm_scheme

        # Although the audiobook is also available in the "AxisNow"
        # format, no second delivery mechanism was created for it, the
        # way it would have been for an ebook.
        assert b"<formatName>AxisNow</formatName>" in data

    def test_bibliographic_parser_blio_format(
        self, axis_files_fixture: AxisFilesFixture
    ):
        # This book is available as 'Blio' but not 'AxisNow'.
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        data = data.replace(b"Acoustik", b"Blio")
        data = data.replace(b"AxisNow", b"No Such Format")

        [[bib, av]] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )
        assert av is not None
        assert bib is not None

        # A book in Blio format is treated as an AxisNow ebook.
        assert Edition.BOOK_MEDIUM == bib.medium
        [axisnow] = bib.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_blio_and_axisnow_format(
        self, axis_files_fixture: AxisFilesFixture
    ):
        # This book is available as both 'Blio' and 'AxisNow'.
        data = axis_files_fixture.sample_data(
            "availability_with_audiobook_fulfillment.xml"
        )
        data = data.replace(b"Acoustik", b"Blio")

        [[bib, av]] = list(
            BibliographicParser().parse(AvailabilityResponse.from_xml(data))
        )
        assert av is not None
        assert bib is not None

        # There is only one FormatData -- 'Blio' and 'AxisNow' mean the same thing.
        assert Edition.BOOK_MEDIUM == bib.medium
        [axisnow] = bib.circulation.formats
        assert None == axisnow.content_type
        assert DeliveryMechanism.AXISNOW_DRM == axisnow.drm_scheme

    def test_bibliographic_parser_unsupported_format(
        self, axis_files_fixture: AxisFilesFixture
    ):
        data = axis_files_fixture.sample_data(
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
        assert [] == bib.circulation.formats

    def test_parse_author_role(self, axis_files_fixture: AxisFilesFixture):
        """Suffixes on author names are turned into roles."""
        author = "Dyssegaard, Elisabeth Kallick (TRN)"
        parse = BibliographicParser._parse_contributor
        c = parse(author)
        assert "Dyssegaard, Elisabeth Kallick" == c.sort_name
        assert (Contributor.Role.TRANSLATOR,) == c.roles

        # A corporate author is given a normal author role.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False)
        assert "Bob, Inc." == c.sort_name
        assert (Contributor.Role.PRIMARY_AUTHOR,) == c.roles

        c = parse(author, primary_author_found=True)
        assert "Bob, Inc." == c.sort_name
        assert (Contributor.Role.AUTHOR,) == c.roles

        # An unknown author type is given an unknown role
        author = "Eve, Mallory (ZZZ)"
        c = parse(author, primary_author_found=False)
        assert "Eve, Mallory" == c.sort_name
        assert (Contributor.Role.UNKNOWN,) == c.roles

        # force_role overwrites whatever other role might be
        # assigned.
        author = "Bob, Inc. (COR)"
        c = parse(
            author, primary_author_found=False, force_role=Contributor.Role.NARRATOR
        )
        assert (Contributor.Role.NARRATOR,) == c.roles

    def test_availability_parser(self, axis_files_fixture: AxisFilesFixture):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = axis_files_fixture.sample_data("tiny_collection.xml")

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

        assert "0003642860" == av1.primary_identifier_data.identifier
        assert 9 == av1.licenses_owned
        assert 9 == av1.licenses_available
        assert 0 == av1.patrons_in_hold_queue
