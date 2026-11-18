import datetime
import random
import string
from datetime import timedelta

import pytest
from freezegun import freeze_time
from frozendict import frozendict
from sqlalchemy.exc import IntegrityError

from palace.util.datetime_helpers import utc_now
from palace.util.exceptions import PalaceValueError
from palace.util.log import LogLevel

from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.sqlalchemy.model.periodical import (
    Periodical,
    PeriodicalVolume,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.util import get_one_or_create
from tests.fixtures.database import DatabaseTransactionFixture


class TestEdition:
    def test_periodical_columns_are_empty_for_a_book(
        self, db: DatabaseTransactionFixture
    ) -> None:
        # A book is not an issue of anything, so the three periodical
        # columns stay empty.
        edition = db.edition(series="The Lord of the Rings")
        edition.series_position = 2
        db.session.flush()

        assert edition.periodical is None
        assert edition.periodical_id is None
        assert edition.periodical_volume is None
        assert edition.periodical_volume_id is None
        assert edition.issue_position is None

        # `series` and `series_position` keep working exactly as before.
        assert edition.series == "The Lord of the Rings"
        assert edition.series_position == 2

    def test_periodical_columns_do_not_touch_series(
        self, db: DatabaseTransactionFixture
    ) -> None:
        # Putting an edition in a periodical leaves `series` and
        # `series_position` alone. The two ideas are kept apart.
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="Volume 1", position=1)
        edition = db.edition(title="March 2026")
        db.session.add_all([periodical, volume])
        edition.set_periodical(periodical, volume, issue_position=3)
        db.session.flush()

        assert edition.periodical == periodical
        assert edition.periodical_volume == volume
        assert edition.issue_position == 3
        assert edition.series is None
        assert edition.series_position is None

    def test_set_periodical(self, db: DatabaseTransactionFixture) -> None:
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="Volume 1", position=1)
        edition = db.edition(title="March 2026")
        db.session.add_all([periodical, volume])

        # A periodical on its own is allowed: we may not know the volume.
        edition.set_periodical(periodical)
        db.session.flush()
        assert edition.periodical == periodical
        assert edition.periodical_volume is None
        assert edition.issue_position is None

        edition.set_periodical(periodical, volume, issue_position=3)
        db.session.flush()
        assert edition.periodical == periodical
        assert edition.periodical_volume == volume
        assert edition.issue_position == 3

        # Passing None clears all three together.
        edition.set_periodical(None)
        db.session.flush()
        assert edition.periodical is None
        assert edition.periodical_volume is None
        assert edition.issue_position is None

    def test_an_issue_position_can_be_a_fraction(
        self, db: DatabaseTransactionFixture
    ) -> None:
        # An issue that sits between two numbered issues keeps its fraction,
        # so it still sorts between them. A whole-number column would round
        # it to 14 and put it on top of issue 14.
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="Volume 1", position=1)
        edition = db.edition(title="Issue 14.1")
        db.session.add_all([periodical, volume])

        edition.set_periodical(periodical, volume, issue_position=14.1)
        db.session.flush()

        # Read it back out of the database rather than off the object we
        # just set, so this is the stored value and not the one in memory.
        db.session.expire(edition)
        assert edition.issue_position == 14.1

    def test_set_periodical_rejects_a_volume_from_another_periodical(
        self, db: DatabaseTransactionFixture
    ) -> None:
        periodical = Periodical(title="The Palace Review")
        other = Periodical(title="The Palace Quarterly")
        other_volume = PeriodicalVolume(periodical=other, label="Volume 1", position=1)
        edition = db.edition(title="March 2026")
        db.session.add_all([periodical, other, other_volume])

        with pytest.raises(PalaceValueError) as excinfo:
            edition.set_periodical(periodical, other_volume)
        assert "belongs to a different periodical" in str(excinfo.value)

        # Nothing was written.
        assert edition.periodical is None
        assert edition.periodical_volume is None

    def test_set_periodical_rejects_a_volume_with_no_periodical(
        self, db: DatabaseTransactionFixture
    ) -> None:
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="Volume 1", position=1)
        edition = db.edition(title="March 2026")
        db.session.add_all([periodical, volume])

        with pytest.raises(PalaceValueError) as excinfo:
            edition.set_periodical(None, volume)
        assert "without also giving it a periodical" in str(excinfo.value)

    def test_a_volume_without_a_periodical_is_refused_by_the_database(
        self, db: DatabaseTransactionFixture
    ) -> None:
        # The same rule the helper enforces is enforced by the database, for
        # anything that sets the columns directly.
        periodical = Periodical(title="The Palace Review")
        volume = PeriodicalVolume(periodical=periodical, label="Volume 1", position=1)
        edition = db.edition(title="March 2026")
        db.session.add_all([periodical, volume])
        db.session.flush()

        edition.periodical_volume = volume
        with pytest.raises(IntegrityError):
            db.session.flush()

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

    def test_for_foreign_id_autocreate_false(self, db: DatabaseTransactionFixture):
        """Verify we can get a data source's view of a foreign id."""
        data_source = "test datasource"
        id = db.fresh_str()
        type = "test id type"

        # The edition was not created because autocreate is False
        edition, was_new = Edition.for_foreign_id(
            db.session, data_source, type, id, autocreate=False
        )
        assert edition is None
        assert was_new is False

        # The associated datasource and identifier were not created either
        ds = DataSource.lookup(db.session, data_source, autocreate=False)
        assert ds is None

        identifier, was_new = Identifier.for_foreign_id(
            db.session, type, id, autocreate=False
        )
        assert identifier is None
        assert was_new is False

        # Create the datasource
        DataSource.lookup(db.session, data_source, autocreate=True)

        # Looking up the edition again with autocreate=False still finds nothing
        edition, was_new = Edition.for_foreign_id(
            db.session, data_source, type, id, autocreate=False
        )
        assert edition is None
        assert was_new is False

        # The associated identifier was not created either
        identifier, was_new = Identifier.for_foreign_id(
            db.session, type, id, autocreate=False
        )
        assert identifier is None
        assert was_new is False

        # Now create the edition
        created_edition, was_new = Edition.for_foreign_id(
            db.session, data_source, type, id, autocreate=True
        )
        assert created_edition is not None
        assert was_new is True
        assert created_edition.data_source.name == data_source
        assert created_edition.primary_identifier.type == type
        assert created_edition.primary_identifier.identifier == id

        # This time looking up the edition with autocreate=False finds it
        edition, was_new = Edition.for_foreign_id(
            db.session, data_source, type, id, autocreate=False
        )
        assert edition is not None
        assert was_new is False
        assert edition == created_edition

    def test_created_and_updated_timestamps(self, db: DatabaseTransactionFixture):
        data_source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        id_ = db.fresh_str()
        type_ = db.fresh_str()

        creation_time = utc_now() - timedelta(days=365)
        with freeze_time(creation_time):
            record, _ = Edition.for_foreign_id(db.session, data_source, type_, id_)

        # The edition automatically gets its created_at timestamp set
        assert record.created_at == creation_time

        # Its updated_at timestamp starts out as None
        assert record.updated_at is None

        # Retrieving the same edition again does not change the timestamps.
        record, was_new = Edition.for_foreign_id(
            db.session, DataSource.GUTENBERG, type_, id_
        )
        assert record.created_at == creation_time
        assert record.updated_at is None

        # If I update the edition, the updated_at timestamp does not update automatically
        record.title = "New Title"
        db.session.flush()
        assert record.created_at == creation_time
        assert record.updated_at is None

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
        l1, new = pool.identifier.add_link(
            Hyperlink.DESCRIPTION, None, overdrive, "text/plain", "F"
        )
        work.set_summary(l1.resource)

        assert work.summary == l1.resource
        assert work.summary_text == "F"

        # Set the work's summary to a string that contains characters that cannot be
        # represented in XML.
        l2, new = pool.identifier.add_link(
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
        l1, new = pool.identifier.add_link(
            Hyperlink.SHORT_DESCRIPTION, None, overdrive, "text/plain", "F"
        )

        overdrive_resource = l1.resource

        # There's a much better description from OCLC Linked Data.
        l2, new = pool.identifier.add_link(
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
        l3, new = pool.identifier.add_link(
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

    # The permanent work IDs below were calculated before periodicals got a
    # grouping category of their own. They are pinned here so that a change to
    # the way we group things can't quietly change the permanent work ID of
    # everything that isn't a periodical.
    PINNED_PERMANENT_WORK_IDS = frozendict(
        {
            Edition.BOOK_MEDIUM: "4823fe05-ec45-73d9-d234-c66756b666d1",
            Edition.AUDIO_MEDIUM: "4823fe05-ec45-73d9-d234-c66756b666d1",
            Edition.MUSIC_MEDIUM: "7fe17ac2-22f7-e210-9f13-a83b044a5330",
            Edition.VIDEO_MEDIUM: "47e474b4-670a-1621-de81-c0b6a60e452a",
            Edition.IMAGE_MEDIUM: "dfafea0e-dd29-e3de-90e3-951723d29aff",
            Edition.COURSEWARE_MEDIUM: "ec94bf94-6785-93e2-035c-f324f4f172c2",
        }
    )

    def test_permanent_work_id_unchanged_for_everything_but_periodicals(
        self, db: DatabaseTransactionFixture
    ):
        # Nothing that isn't a periodical changes its permanent work ID.
        for medium, expected in self.PINNED_PERMANENT_WORK_IDS.items():
            edition = db.edition(title="Fahrenheit 451", authors="Bradbury, Ray")
            edition.medium = medium
            edition.calculate_permanent_work_id()
            assert edition.permanent_work_id == expected

    def test_publication_date_does_not_affect_a_book(
        self, db: DatabaseTransactionFixture
    ):
        # The publication date and the issue position only count for
        # periodicals. A book ignores both.
        edition = db.edition(title="Fahrenheit 451", authors="Bradbury, Ray")
        edition.calculate_permanent_work_id()
        before = edition.permanent_work_id

        edition.published = datetime.date(2024, 3, 1)
        edition.issue_position = 3
        edition.calculate_permanent_work_id()
        assert edition.permanent_work_id == before
        assert before == self.PINNED_PERMANENT_WORK_IDS[Edition.BOOK_MEDIUM]

    def _issue(
        self,
        db: DatabaseTransactionFixture,
        published: datetime.date | None = None,
        issue_position: int | None = None,
        data_source_name: str = DataSource.GUTENBERG,
        identifier_type: str = Identifier.GUTENBERG_ID,
        with_open_access_download: bool = False,
        volume: PeriodicalVolume | None = None,
    ) -> tuple[Edition, LicensePool | None]:
        """Make one issue of The Palace Review.

        Every issue carries the same title and the same author, which is what
        made them all collapse into one Work.
        """
        made = db.edition(
            title="The Palace Review",
            authors="Palace, Editors",
            data_source_name=data_source_name,
            identifier_type=identifier_type,
            with_open_access_download=with_open_access_download,
        )
        edition, pool = made if with_open_access_download else (made, None)
        edition.medium = Edition.PERIODICAL_MEDIUM
        edition.published = published
        edition.issue_position = issue_position
        edition.periodical_volume = volume
        # The database will not take a volume without the periodical it
        # belongs to, so take the periodical from the volume.
        edition.periodical = volume.periodical if volume is not None else None
        edition.calculate_permanent_work_id()
        return edition, pool

    def test_twelve_issues_get_twelve_permanent_work_ids(
        self, db: DatabaseTransactionFixture
    ):
        # A year of a monthly periodical: same title, same author, twelve
        # issues.
        editions = [
            self._issue(db, published=datetime.date(2024, month, 1))[0]
            for month in range(1, 13)
        ]
        permanent_work_ids = {edition.permanent_work_id for edition in editions}
        assert len(permanent_work_ids) == 12
        assert None not in permanent_work_ids

    def test_twelve_issues_get_twelve_works(self, db: DatabaseTransactionFixture):
        # Open access pools are grouped into Works by permanent work ID, so
        # this is where the issues used to collapse into one Work.
        works = set()
        for month in range(1, 13):
            edition, pool = self._issue(
                db,
                published=datetime.date(2024, month, 1),
                with_open_access_download=True,
            )
            assert pool is not None
            work, _ = pool.calculate_work()
            works.add(work)

        assert len(works) == 12

    def test_the_same_issue_from_two_sources_is_one_work(
        self, db: DatabaseTransactionFixture
    ):
        # Two sources, one issue. These should still come together.
        published = datetime.date(2024, 3, 1)
        first, first_pool = self._issue(
            db,
            published=published,
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            with_open_access_download=True,
        )
        second, second_pool = self._issue(
            db,
            published=published,
            data_source_name=DataSource.STANDARD_EBOOKS,
            identifier_type=Identifier.URI,
            with_open_access_download=True,
        )

        assert first.permanent_work_id == second.permanent_work_id
        assert first_pool is not None and second_pool is not None
        first_work, _ = first_pool.calculate_work()
        second_work, _ = second_pool.calculate_work()
        assert first_work == second_work

    def test_issue_position_tells_issues_apart_without_a_date(
        self, db: DatabaseTransactionFixture
    ):
        # Not every source gives us a publication date. The position of the
        # issue within its volume will do instead.
        first, _ = self._issue(db, issue_position=1)
        second, _ = self._issue(db, issue_position=2)
        assert first.permanent_work_id != second.permanent_work_id

    def test_an_issue_with_no_date_and_no_position(
        self, db: DatabaseTransactionFixture
    ):
        # With nothing to tell them apart, two issues still share a permanent
        # work ID. We have no way to do better, and guessing would split
        # issues that are really the same.
        first, _ = self._issue(db)
        second, _ = self._issue(db)
        assert first.permanent_work_id == second.permanent_work_id

        # It is still not the ID it would have had as a book, because a
        # periodical is no longer grouped with books.
        assert first.permanent_work_id != "1229788b-6b12-bb9c-99bf-b255379303e3"

    def test_the_same_issue_position_in_two_volumes(
        self, db: DatabaseTransactionFixture
    ):
        # An issue position only counts within a volume. Issue 3 of volume 1
        # and issue 3 of volume 2 are different issues, and neither has a
        # publication date to say so.
        periodical = Periodical(title="The Palace Review")
        first_volume = PeriodicalVolume(
            periodical=periodical, label="Volume 1", position=1
        )
        second_volume = PeriodicalVolume(
            periodical=periodical, label="Volume 2", position=2
        )

        first, _ = self._issue(db, issue_position=3, volume=first_volume)
        second, _ = self._issue(db, issue_position=3, volume=second_volume)
        assert first.permanent_work_id != second.permanent_work_id

        # The same issue of the same volume still comes out the same.
        again, _ = self._issue(db, issue_position=3, volume=first_volume)
        assert again.permanent_work_id == first.permanent_work_id

    def test_a_volume_with_no_position_falls_back_to_its_label(
        self, db: DatabaseTransactionFixture
    ):
        # Not every source numbers its volumes. The label will do.
        periodical = Periodical(title="The Palace Review")
        spring = PeriodicalVolume(periodical=periodical, label="Spring 2024")
        summer = PeriodicalVolume(periodical=periodical, label="Summer 2024")

        first, _ = self._issue(db, issue_position=1, volume=spring)
        second, _ = self._issue(db, issue_position=1, volume=summer)
        assert first.permanent_work_id != second.permanent_work_id

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

    def test_choose_cover_uses_full_image_as_thumbnail_fallback(
        self, db: DatabaseTransactionFixture, caplog: pytest.LogCaptureFixture
    ):
        """When choose_cover finds a full-size image but no thumbnail,
        it falls back to using the full-size image as the thumbnail.
        """
        caplog.set_level(LogLevel.info)

        edition = db.edition()

        # Create a full-size image without any thumbnail
        main_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.IMAGE,
            "http://example.com/cover.jpg",
            edition.data_source,
            Representation.JPEG_MEDIA_TYPE,
        )

        # Give it a representation so choose_cover can use it
        main_image.resource.representation, _ = db.representation(
            media_type=Representation.JPEG_MEDIA_TYPE, url=main_image.resource.url
        )

        # Initially no cover is set
        assert None == edition.cover_full_url
        assert None == edition.cover_thumbnail_url

        # choose_cover should find the image and use it for both
        edition.choose_cover()

        # The full-size image should be used for both full and thumbnail
        assert main_image.resource.url == edition.cover_full_url
        assert main_image.resource.url == edition.cover_thumbnail_url

        # An info-level log message should indicate that the full-size image
        # is being used as the thumbnail
        assert "Using full-size image as thumbnail fallback" in caplog.text
