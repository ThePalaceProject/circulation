from __future__ import annotations

from palace.manager.scripts.work import (
    ReclassifyWorksForUncheckedSubjectsScript,
    WorkClassificationScript,
    WorkProcessingScript,
)
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture


class TestWorkProcessingScript:
    def test_make_query(self, db: DatabaseTransactionFixture):
        # Create two Gutenberg works and one Overdrive work
        g1 = db.work(with_license_pool=True, with_open_access_download=True)
        g2 = db.work(with_license_pool=True, with_open_access_download=True)

        overdrive_edition = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )[0]
        overdrive_work = db.work(presentation_edition=overdrive_edition)

        ugi_edition = db.edition(
            data_source_name=DataSource.UNGLUE_IT,
            identifier_type=Identifier.URI,
            with_license_pool=True,
        )[0]
        unglue_it = db.work(presentation_edition=ugi_edition)

        se_edition = db.edition(
            data_source_name=DataSource.STANDARD_EBOOKS,
            identifier_type=Identifier.URI,
            with_license_pool=True,
        )[0]
        standard_ebooks = db.work(presentation_edition=se_edition)

        everything = WorkProcessingScript.make_query(db.session, None, None, None)
        assert {g1, g2, overdrive_work, unglue_it, standard_ebooks} == set(
            everything.all()
        )

        all_gutenberg = WorkProcessingScript.make_query(
            db.session, Identifier.GUTENBERG_ID, [], None
        )
        assert {g1, g2} == set(all_gutenberg.all())

        one_gutenberg = WorkProcessingScript.make_query(
            db.session, Identifier.GUTENBERG_ID, [g1.license_pools[0].identifier], None
        )
        assert [g1] == one_gutenberg.all()

        one_standard_ebook = WorkProcessingScript.make_query(
            db.session, Identifier.URI, [], DataSource.STANDARD_EBOOKS
        )
        assert [standard_ebooks] == one_standard_ebook.all()


class TestReclassifyWorksForUncheckedSubjectsScript:
    def test_constructor(self, db: DatabaseTransactionFixture):
        """Make sure that we're only going to classify works
        with unchecked subjects.
        """
        script = ReclassifyWorksForUncheckedSubjectsScript(db.session)
        assert (
            WorkClassificationScript.policy
            == ReclassifyWorksForUncheckedSubjectsScript.policy
        )
        assert 100 == script.batch_size

        # Assert all joins have been included in the Order By
        ordered_by = script.query._order_by_clauses
        for join in [Work, LicensePool, Identifier, Classification]:
            assert join.id in ordered_by  # type: ignore[attr-defined]

        assert Work.id in ordered_by

    def test_paginate(self, db: DatabaseTransactionFixture):
        """Pagination is changed to be row-wise comparison
        Ensure we are paginating correctly within the same Subject page"""
        subject = db.subject(Subject.AXIS_360_AUDIENCE, "Any")
        works = []
        for i in range(20):
            work: Work = db.work(with_license_pool=True)
            db.classification(
                work.presentation_edition.primary_identifier,
                subject,
                work.license_pools[0].data_source,
            )
            works.append(work)

        script = ReclassifyWorksForUncheckedSubjectsScript(db.session)
        script.batch_size = 1
        for ix, [work] in enumerate(script.paginate_query(script.query)):
            # We are coming in via "id" order
            assert work == works[ix]
        assert ix == 19

        other_subject = db.subject(Subject.BISAC, "Any")
        last_work = works[-1]
        db.classification(
            last_work.presentation_edition.primary_identifier,
            other_subject,
            last_work.license_pools[0].data_source,
        )
        script.batch_size = 100
        next_works = next(script.paginate_query(script.query))
        # Works are only iterated over ONCE per loop
        assert len(next_works) == 20

        # A checked subjects work is not included
        not_work = db.work(with_license_pool=True)
        another_subject = db.subject(Subject.DDC, "Any")
        db.classification(
            not_work.presentation_edition.primary_identifier,
            another_subject,
            not_work.license_pools[0].data_source,
        )
        another_subject.checked = True
        db.session.commit()
        next_works = next(script.paginate_query(script.query))
        assert len(next_works) == 20
        assert not_work not in next_works

    def test_subject_checked(self, db: DatabaseTransactionFixture):
        subject = db.subject(Subject.AXIS_360_AUDIENCE, "Any")
        assert subject.checked == False

        works = []
        for i in range(10):
            work: Work = db.work(with_license_pool=True)
            db.classification(
                work.presentation_edition.primary_identifier,
                subject,
                work.license_pools[0].data_source,
            )
            works.append(work)

        script = ReclassifyWorksForUncheckedSubjectsScript(db.session)
        script.run()
        db.session.refresh(subject)
        assert subject.checked == True
