import random
import string
from datetime import timedelta

from freezegun import freeze_time

from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestEdition:
    def test_audio_mpeg_is_audiobook(self):
        assert Edition.AUDIO_MEDIUM == Edition.medium_from_media_type("audio/mpeg")

    def test_medium_from_media_type(self):
        # Verify that we can guess a value for Edition.medium from a
        # media type.

        m = Edition.medium_from_media_type
        for audio_type in MediaTypes.AUDIOBOOK_MEDIA_TYPES:
            assert Edition.AUDIO_MEDIUM == m(audio_type)
            assert Edition.AUDIO_MEDIUM == m(audio_type + ";param=value")

        for book_type in MediaTypes.BOOK_MEDIA_TYPES:
            assert Edition.BOOK_MEDIUM == m(book_type)
            assert Edition.BOOK_MEDIUM == m(book_type + ";param=value")

        assert Edition.BOOK_MEDIUM == m(DeliveryMechanism.ADOBE_DRM)

    def test_license_pools(self, db: DatabaseTransactionFixture):
        # Here are two collections that provide access to the same book.
        c1 = db.collection()
        c2 = db.collection()

        edition, lp1 = db.edition(with_license_pool=True)
        lp2 = db.licensepool(edition=edition, collection=c2)

        # Two LicensePools for the same work.
        assert lp1.identifier == lp2.identifier

        # Edition.license_pools contains both.
        assert {lp1, lp2} == set(edition.license_pools)

    def test_author_contributors(self, db: DatabaseTransactionFixture):
        data_source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        id = db.fresh_str()
        type = Identifier.GUTENBERG_ID

        edition, was_new = Edition.for_foreign_id(db.session, data_source, type, id)

        # We've listed the same person as primary author and author.
        [alice], ignore = Contributor.lookup(db.session, "Adder, Alice")
        edition.add_contributor(
            alice, [Contributor.Role.AUTHOR, Contributor.Role.PRIMARY_AUTHOR]
        )

        # We've listed a different person as illustrator.
        [bob], ignore = Contributor.lookup(db.session, "Bitshifter, Bob")
        edition.add_contributor(bob, [Contributor.Role.ILLUSTRATOR])

        # Both contributors show up in .contributors.
        assert {alice, bob} == edition.contributors

        # Only the author shows up in .author_contributors, and she
        # only shows up once.
        assert [alice] == edition.author_contributors

    def test_for_foreign_id(self, db: DatabaseTransactionFixture):
        """Verify we can get a data source's view of a foreign id."""
        data_source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        id = "549"
        type = Identifier.GUTENBERG_ID

        record, was_new = Edition.for_foreign_id(db.session, data_source, type, id)
        assert data_source == record.data_source
        identifier = record.primary_identifier
        assert id == identifier.identifier
        assert type == identifier.type
        assert True == was_new
        assert [identifier] == record.equivalent_identifiers()

        # We can get the same work record by providing only the name
        # of the data source.
        record, was_new = Edition.for_foreign_id(
            db.session, DataSource.GUTENBERG, type, id
        )
        assert data_source == record.data_source
        assert identifier == record.primary_identifier
        assert False == was_new

    def test_created_and_updated_timestamps(self, db: DatabaseTransactionFixture):
        data_source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        id_ = db.fresh_str()
        type_ = db.fresh_str()

        creation_time = utc_now() - timedelta(days=365)
        with freeze_time(creation_time):
            record, _ = Edition.for_foreign_id(db.session, data_source, type_, id_)

        # The edition automatically gets timestamps set on it
        assert record.created_at == creation_time
        assert record.updated_at == creation_time

        # Retrieving the same edition again does not change the timestamps.
        record, was_new = Edition.for_foreign_id(
            db.session, DataSource.GUTENBERG, type_, id_
        )
        assert record.created_at == creation_time
        assert record.updated_at == creation_time

        # If I update the edition, the updated_at timestamp changes automatically
        update_time = utc_now()
        with freeze_time(update_time):
            record.title = "New Title"
            db.session.flush()
        assert record.created_at == creation_time
        assert record.updated_at == update_time

        # If I manually set the updated_at timestamp, it does not change automatically
        manual_update_time = utc_now() + timedelta(days=1)
        with freeze_time(manual_update_time):
            record.title = "Another New Title"
            record.updated_at = manual_update_time
            record.series = "New Series"
            db.session.flush()
        assert record.created_at == creation_time
        assert record.updated_at == manual_update_time

    def test_sort_by_priority(self, db: DatabaseTransactionFixture):
        # Make editions created by the license source, the metadata
        # wrangler, and library staff.
        admin = db.edition(
            data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False
        )
        od = db.edition(data_source_name=DataSource.OVERDRIVE, with_license_pool=False)
        mw = db.edition(
            data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False
        )

        # Create an invalid edition with no data source. (This shouldn't
        # happen.)
        no_data_source = db.edition(with_license_pool=False)
        no_data_source.data_source = None

        def ids(l):
            return [x for x in l]

        # The invalid edition is the lowest priority. The admin
        # interface and metadata wrangler take precedence over any
        # other data sources.
        expect = [no_data_source, od, mw, admin]
        actual = Edition.sort_by_priority(expect)
        assert ids(expect) == ids(actual)

        # If you specify which data source is associated with the
        # license for the book, you will boost its priority above that
        # of the metadata wrangler.
        expect = [no_data_source, mw, od, admin]
        actual = Edition.sort_by_priority(expect, od.data_source)
        assert ids(expect) == ids(actual)

    def test_equivalent_identifiers(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        identifier = db.identifier()
        session = db.session
        data_source = DataSource.lookup(session, DataSource.OCLC)

        identifier.equivalent_to(data_source, edition.primary_identifier, 0.6)

        policy = PresentationCalculationPolicy(equivalent_identifier_threshold=0.5)
        assert {identifier, edition.primary_identifier} == set(
            edition.equivalent_identifiers(policy=policy)
        )

        policy = PresentationCalculationPolicy(equivalent_identifier_threshold=0.7)
        assert {edition.primary_identifier} == set(
            edition.equivalent_identifiers(policy=policy)
        )

    def test_recursive_edition_equivalence(self, db: DatabaseTransactionFixture):
        # Here's a Edition for a Project Gutenberg text.
        gutenberg, gutenberg_pool = db.edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Original Gutenberg text",
        )

        # Here's a Edition for an Open Library text.
        open_library, open_library_pool = db.edition(
            data_source_name=DataSource.OPEN_LIBRARY,
            identifier_type=Identifier.OPEN_LIBRARY_ID,
            identifier_id="W1111",
            with_open_access_download=True,
            title="Open Library record",
        )

        # We've learned from OCLC Classify that the Gutenberg text is
        # equivalent to a certain OCLC Number. We've learned from OCLC
        # Linked Data that the Open Library text is equivalent to the
        # same OCLC Number.
        session = db.session
        oclc_classify = DataSource.lookup(session, DataSource.OCLC)
        oclc_linked_data = DataSource.lookup(session, DataSource.OCLC_LINKED_DATA)

        oclc_number, ignore = Identifier.for_foreign_id(
            session, Identifier.OCLC_NUMBER, "22"
        )
        gutenberg.primary_identifier.equivalent_to(oclc_classify, oclc_number, 1)
        open_library.primary_identifier.equivalent_to(oclc_linked_data, oclc_number, 1)

        # Here's a Edition for a Recovering the Classics cover.
        web_source = DataSource.lookup(session, DataSource.WEB)
        recovering, ignore = Edition.for_foreign_id(
            session,
            web_source,
            Identifier.URI,
            "http://recoveringtheclassics.com/pride-and-prejudice.jpg",
        )
        recovering.title = "Recovering the Classics cover"

        # We've manually associated that Edition's URI directly
        # with the Project Gutenberg text.
        manual = DataSource.lookup(session, DataSource.MANUAL)
        gutenberg.primary_identifier.equivalent_to(
            manual, recovering.primary_identifier, 1
        )

        # Finally, here's a completely unrelated Edition, which
        # will not be showing up.
        gutenberg2, gutenberg2_pool = db.edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="2",
            with_open_access_download=True,
            title="Unrelated Gutenberg record.",
        )

        # When we call equivalent_editions on the Project Gutenberg
        # Edition, we get three Editions: the Gutenberg record
        # itself, the Open Library record, and the Recovering the
        # Classics record.
        #
        # We get the Open Library record because it's associated with
        # the same OCLC Number as the Gutenberg record. We get the
        # Recovering the Classics record because it's associated
        # directly with the Gutenberg record.
        results = list(gutenberg.equivalent_editions())
        assert 3 == len(results)
        assert gutenberg in results
        assert open_library in results
        assert recovering in results

        # Here's a Work that incorporates one of the Gutenberg records.
        work = db.work()
        work.license_pools.extend([gutenberg2_pool])

        # Its set-of-all-editions contains only one record.
        assert 1 == work.all_editions().count()

        # If we add the other Gutenberg record to it, then its
        # set-of-all-editions is extended by that record, *plus*
        # all the Editions equivalent to that record.
        work.license_pools.extend([gutenberg_pool])
        assert 4 == work.all_editions().count()

    def test_calculate_presentation_title(self, db: DatabaseTransactionFixture):
        wr = db.edition(title="The Foo")
        wr.calculate_presentation()
        assert "Foo, The" == wr.sort_title

        wr = db.edition(title="A Foo")
        wr.calculate_presentation()
        assert "Foo, A" == wr.sort_title

    def test_calculate_presentation_missing_author(
        self, db: DatabaseTransactionFixture
    ):
        wr = db.edition()
        db.session.delete(wr.contributions[0])
        db.session.commit()
        wr.calculate_presentation()
        assert "[Unknown]" == wr.sort_author
        assert "[Unknown]" == wr.author

    def test_calculate_presentation_author(self, db: DatabaseTransactionFixture):
        bob, ignore = db.contributor(sort_name="Bitshifter, Bob")
        wr = db.edition(authors=bob.sort_name)
        wr.calculate_presentation()
        assert "Bob Bitshifter" == wr.author
        assert "Bitshifter, Bob" == wr.sort_author

        bob.display_name = "Bob A. Bitshifter"
        wr.calculate_presentation()
        assert "Bob A. Bitshifter" == wr.author
        assert "Bitshifter, Bob" == wr.sort_author

        kelly, ignore = db.contributor(sort_name="Accumulator, Kelly")
        wr.add_contributor(kelly, Contributor.Role.AUTHOR)
        wr.calculate_presentation()
        assert "Kelly Accumulator, Bob A. Bitshifter" == wr.author
        assert "Accumulator, Kelly ; Bitshifter, Bob" == wr.sort_author

    def test_calculate_presentation_very_long_author(
        self, db: DatabaseTransactionFixture
    ):
        authors = []

        # author names should be unique and not similar to ensure that the
        # test mirrors the types of long author lists we'd expect in real data.
        def generate_random_author():
            return "".join(
                random.choices(
                    string.ascii_uppercase + string.ascii_lowercase + string.digits,
                    k=25,
                )
            )

        for i in range(0, 500):
            author, ignore = db.contributor(
                sort_name=", ".join(
                    [
                        generate_random_author(),
                        generate_random_author(),
                    ]
                )
            )
            authors.append(author.sort_name)

        untruncated_sort_authors = ", ".join([x for x in sorted(authors)])
        wr = db.edition(authors=authors)
        wr.calculate_presentation()
        db.session.commit()

        def do_check(original_str: str, truncated_str: str):
            assert (
                len(truncated_str)
                == Edition.SAFE_AUTHOR_FIELD_LENGTH_TO_AVOID_PG_INDEX_ERROR
            )
            assert truncated_str.endswith("...")
            assert not original_str.endswith("...")
            assert (
                len(original_str)
                > Edition.SAFE_AUTHOR_FIELD_LENGTH_TO_AVOID_PG_INDEX_ERROR
            )

        do_check(untruncated_sort_authors, wr.sort_author)
        # Since we'd expect the sort_author and auth to be equal (since sort_author is assigned to the
        # author field by default if no author is specified) we should verify that the author field also
        # passes the check.
        do_check(untruncated_sort_authors, wr.author)

    def test_calculate_presentation_shortish_author(
        self, db: DatabaseTransactionFixture
    ):
        authors = []
        author, ignore = db.contributor(sort_name=f"AuthorLast, AuthorFirst")
        authors.append(author.sort_name)
        wr = db.edition(authors=authors)
        author, sort_author = wr.calculate_author()
        wr.calculate_presentation()
        db.session.commit()

        def do_check(original_str: str, calculated_str: str):
            assert calculated_str == original_str
            assert not calculated_str.endswith("...")
            assert (
                len(original_str)
                <= Edition.SAFE_AUTHOR_FIELD_LENGTH_TO_AVOID_PG_INDEX_ERROR
            )
            assert not original_str.endswith("...")

        do_check(author, wr.author)
        do_check(sort_author, wr.sort_author)

    def test_set_summary(self, db: DatabaseTransactionFixture):
        e, pool = db.edition(with_license_pool=True)
        work = db.work(presentation_edition=e)
        overdrive = DataSource.lookup(db.session, DataSource.OVERDRIVE)

        # Set the work's summary.
        l1, new = pool.add_link(
            Hyperlink.DESCRIPTION, None, overdrive, "text/plain", "F"
        )
        work.set_summary(l1.resource)

        assert work.summary == l1.resource
        assert work.summary_text == "F"

        # Set the work's summary to a string that contains characters that cannot be
        # represented in XML.
        l2, new = pool.add_link(
            Hyperlink.DESCRIPTION,
            None,
            overdrive,
            "text/plain",
            "\u0000💣ü🔥\u0001\u000c",
        )
        work.set_summary(l2.resource)
        assert work.summary_text == "💣ü🔥"

        # Remove the summary.
        work.set_summary(None)

        assert work.summary is None
        assert work.summary_text == ""

    def test_calculate_evaluate_summary_quality_with_privileged_data_sources(
        self, db: DatabaseTransactionFixture
    ):
        e, pool = db.edition(with_license_pool=True)
        oclc = DataSource.lookup(
            db.session, DataSource.OCLC_LINKED_DATA, autocreate=True
        )
        overdrive = DataSource.lookup(db.session, DataSource.OVERDRIVE, autocreate=True)

        # There's a perfunctory description from Overdrive.
        l1, new = pool.add_link(
            Hyperlink.SHORT_DESCRIPTION, None, overdrive, "text/plain", "F"
        )

        overdrive_resource = l1.resource

        # There's a much better description from OCLC Linked Data.
        l2, new = pool.add_link(
            Hyperlink.DESCRIPTION,
            None,
            oclc,
            "text/plain",
            """Nothing about working with his former high school crush, Stephanie Stephens, is ideal. Still, if Aaron Caruthers intends to save his grandmother's bakery, he must. Good thing he has a lot of ideas he can't wait to implement. He never imagines Stephanie would have her own ideas for the business. Or that they would clash with his!""",
        )
        oclc_resource = l2.resource

        # In a head-to-head evaluation, the OCLC Linked Data description wins.
        ids = [e.primary_identifier.id]
        champ1, resources = Identifier.evaluate_summary_quality(db.session, ids)

        assert {overdrive_resource, oclc_resource} == set(resources)
        assert oclc_resource == champ1

        # But if we say that Overdrive is the privileged data source, it wins
        # automatically. The other resource isn't even considered.
        champ2, resources2 = Identifier.evaluate_summary_quality(
            db.session, ids, [overdrive]
        )
        assert overdrive_resource == champ2
        assert [overdrive_resource] == resources2

        # If we say that some other data source is privileged, and
        # there are no descriptions from that data source, a
        # head-to-head evaluation is performed, and OCLC Linked Data
        # wins.
        threem = DataSource.lookup(db.session, DataSource.BIBLIOTHECA, autocreate=True)
        champ3, resources3 = Identifier.evaluate_summary_quality(
            db.session, ids, [threem]
        )
        assert {overdrive_resource, oclc_resource} == set(resources3)
        assert oclc_resource == champ3

        # If there are two privileged data sources and there's no
        # description from the first, the second is used.
        champ4, resources4 = Identifier.evaluate_summary_quality(
            db.session, ids, [threem, overdrive]
        )
        assert [overdrive_resource] == resources4
        assert overdrive_resource == champ4

        # Even an empty string wins if it's from the most privileged data source.
        # This is not a silly example.  The librarian may choose to set the description
        # to an empty string in the admin inteface, to override a bad overdrive/etc. description.
        staff = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF, autocreate=True)
        l3, new = pool.add_link(
            Hyperlink.SHORT_DESCRIPTION, None, staff, "text/plain", ""
        )
        staff_resource = l3.resource

        champ5, resources5 = Identifier.evaluate_summary_quality(
            db.session, ids, [staff, overdrive]
        )
        assert [staff_resource] == resources5
        assert staff_resource == champ5

    def test_calculate_presentation_cover(self, db: DatabaseTransactionFixture):
        # Here's a cover image with a thumbnail.
        representation, ignore = get_one_or_create(
            db.session, Representation, url="http://cover"
        )
        representation.media_type = Representation.JPEG_MEDIA_TYPE
        representation.mirrored_at = utc_now()
        representation.mirror_url = "http://mirror/cover"
        thumb, ignore = get_one_or_create(
            db.session, Representation, url="http://thumb"
        )
        thumb.media_type = Representation.JPEG_MEDIA_TYPE
        thumb.mirrored_at = utc_now()
        thumb.mirror_url = "http://mirror/thumb"
        thumb.thumbnail_of_id = representation.id

        # Verify that a cover for the edition's primary identifier is used.
        e, pool = db.edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(
            Hyperlink.IMAGE, "http://cover", e.data_source
        )
        link.resource.representation = representation
        e.calculate_presentation()
        assert "http://mirror/cover" == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url

        # Verify that a cover will be used even if it's some
        # distance away along the identifier-equivalence line.
        e, pool = db.edition(with_license_pool=True)
        oclc_classify = DataSource.lookup(db.session, DataSource.OCLC, autocreate=True)
        oclc_number, ignore = Identifier.for_foreign_id(
            db.session, Identifier.OCLC_NUMBER, "22"
        )
        e.primary_identifier.equivalent_to(oclc_classify, oclc_number, 1)
        link, ignore = oclc_number.add_link(
            Hyperlink.IMAGE, "http://cover", oclc_classify
        )
        link.resource.representation = representation
        e.calculate_presentation()
        assert "http://mirror/cover" == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url

        # Verify that a nearby cover takes precedence over a
        # faraway cover.
        link, ignore = e.primary_identifier.add_link(
            Hyperlink.IMAGE, "http://nearby-cover", e.data_source
        )
        nearby, ignore = get_one_or_create(
            db.session, Representation, url=link.resource.url
        )
        nearby.media_type = Representation.JPEG_MEDIA_TYPE
        nearby.mirrored_at = utc_now()
        nearby.mirror_url = "http://mirror/nearby-cover"
        link.resource.representation = nearby
        nearby_thumb, ignore = get_one_or_create(
            db.session, Representation, url="http://nearby-thumb"
        )
        nearby_thumb.media_type = Representation.JPEG_MEDIA_TYPE
        nearby_thumb.mirrored_at = utc_now()
        nearby_thumb.mirror_url = "http://mirror/nearby-thumb"
        nearby_thumb.thumbnail_of_id = nearby.id
        e.calculate_presentation()
        assert "http://mirror/nearby-cover" == e.cover_full_url
        assert "http://mirror/nearby-thumb" == e.cover_thumbnail_url

        # Verify that a thumbnail is used even if there's
        # no full-sized cover.
        e, pool = db.edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumb", e.data_source
        )
        link.resource.representation = thumb
        e.calculate_presentation()
        assert None == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url

    def test_no_permanent_work_id_for_edition_without_title_or_medium(
        self, db: DatabaseTransactionFixture
    ):
        # An edition with no title or medium is not assigned a permanent work
        # ID.
        edition = db.edition()
        assert None == edition.permanent_work_id

        edition.title = ""
        edition.calculate_permanent_work_id()
        assert None == edition.permanent_work_id

        edition.title = "something"
        edition.calculate_permanent_work_id()
        assert None != edition.permanent_work_id

        edition.medium = None
        edition.calculate_permanent_work_id()
        assert None == edition.permanent_work_id

    def test_choose_cover_can_choose_full_image_and_thumbnail_separately(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()

        # This edition has a full-sized image and a thumbnail image,
        # but there is no evidence that they are the _same_ image.
        main_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.IMAGE,
            "http://main/",
            edition.data_source,
            Representation.PNG_MEDIA_TYPE,
        )
        thumbnail_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE,
            "http://thumbnail/",
            edition.data_source,
            Representation.PNG_MEDIA_TYPE,
        )

        # Nonetheless, Edition.choose_cover() will assign the
        # potentially unrelated images to the Edition, because there
        # is no better option.
        edition.choose_cover()
        assert main_image.resource.url == edition.cover_full_url
        assert thumbnail_image.resource.url == edition.cover_thumbnail_url

        # If there is a clear indication that one of the thumbnails
        # associated with the identifier is a thumbnail _of_ the
        # full-sized image...
        thumbnail_2, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE,
            "http://thumbnail2/",
            edition.data_source,
            Representation.PNG_MEDIA_TYPE,
        )
        thumbnail_2.resource.representation.thumbnail_of = (
            main_image.resource.representation
        )
        edition.choose_cover()

        # ...That thumbnail will be chosen in preference to the
        # possibly unrelated thumbnail.
        assert main_image.resource.url == edition.cover_full_url
        assert thumbnail_2.resource.url == edition.cover_thumbnail_url
