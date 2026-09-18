from unittest.mock import patch

import pytest

from palace.manager.celery.tasks import work as work_tasks
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


def test_work_ids_with_unchecked_subjects(db: DatabaseTransactionFixture):
    """_work_ids_with_unchecked_subjects returns IDs for works linked to at
    least one unchecked subject, deduplicates, and excludes works whose
    subjects are all checked."""
    unchecked = db.subject(Subject.BISAC, "FBJUV000000")
    checked = db.subject(Subject.BISAC, "FIC000000")
    checked.checked = True

    # Work with one unchecked subject — should appear.
    work_a: Work = db.work(with_license_pool=True)
    db.classification(
        work_a.presentation_edition.primary_identifier,
        unchecked,
        work_a.license_pools[0].data_source,
    )

    # Work with both an unchecked and a checked subject — should appear once.
    work_b: Work = db.work(with_license_pool=True)
    for subject in [unchecked, checked]:
        db.classification(
            work_b.presentation_edition.primary_identifier,
            subject,
            work_b.license_pools[0].data_source,
        )

    # Work with only a checked subject — should not appear.
    work_c: Work = db.work(with_license_pool=True)
    db.classification(
        work_c.presentation_edition.primary_identifier,
        checked,
        work_c.license_pools[0].data_source,
    )

    # Work with no classifications — should not appear.
    work_d: Work = db.work(with_license_pool=True)

    db.session.commit()

    result = work_tasks._work_ids_with_unchecked_subjects(db.session)

    assert sorted(result) == sorted([work_a.id, work_b.id])
    assert work_c.id not in result
    assert work_d.id not in result
    # Each work appears exactly once even when linked to multiple unchecked subjects.
    assert len(result) == len(set(result))


def test_subject_checked(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    """Test that classify_unchecked_subjects recalculates works with unchecked subjects."""
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

    with patch.object(Work, "calculate_presentation") as calc_pres:
        work_tasks.classify_unchecked_subjects.delay().wait()

    # Should have been called once for each work
    assert calc_pres.call_count == 10
    # Should use recalculate_classification policy
    for call_obj in calc_pres.call_args_list:
        policy = call_obj[1]["policy"]
        assert policy.classify is True
        assert policy.choose_edition is False

    # now verify that no recalculation occurs when the subject.checked property is true.
    subject.checked = True
    db.session.commit()
    with patch.object(Work, "calculate_presentation") as calc_pres:
        work_tasks.classify_unchecked_subjects.delay().wait()
    assert calc_pres.call_count == 0


def test_reclassify_null_audience_works(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    """reclassify_null_audience_works calls calculate_presentation for works with NULL audience
    and leaves works with a non-NULL audience untouched."""
    null_works = []
    for _ in range(3):
        work: Work = db.work(with_license_pool=True)
        work.audience = None
        null_works.append(work)

    adult_work: Work = db.work(with_license_pool=True)
    # audience defaults to "Adult" in db.work()

    db.session.commit()

    with patch.object(Work, "calculate_presentation") as calc_pres:
        work_tasks.reclassify_null_audience_works.delay().wait()

    # Only the three null-audience works should trigger calculate_presentation.
    assert calc_pres.call_count == 3

    # Verify the recalculate_classification policy is used for each call.
    for call_obj in calc_pres.call_args_list:
        policy = call_obj[1]["policy"]
        assert policy.classify is True
        assert policy.choose_edition is False


@pytest.mark.parametrize(
    "identifier,name",
    [
        pytest.param("INFEN000", "English literature", id="language_category"),
        pytest.param(
            "INFENUSA", "American and Canadian literature", id="territory_category"
        ),
        pytest.param("FBSACT000000", "News and investigations", id="vendor_code"),
        pytest.param("FSHUM000000N", "Human science", id="vendor_code_with_n_suffix"),
        pytest.param("SOCO32000", None, id="malformed_code_without_name"),
        pytest.param("FBZZZ000000", "Historical", id="shape_valid_but_nonexistent"),
        pytest.param("FBFIC014000", "Historical", id="stale_canonical_fiction_code"),
    ],
)
def test_reset_non_bisac_nonfiction_subjects(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    identifier: str,
    name: str | None,
) -> None:
    """Subjects stored as nonfiction that the classifier no longer scores that
    way are marked unchecked, so classify_unchecked_subjects re-scores them.

    Covers both codes that are not BISAC at all and codes that merely look like
    one (FBZZZ000000), which a pattern-based predicate would wrongly accept.
    """
    subject = db.subject(Subject.BISAC, identifier)
    subject.name = name
    subject.fiction = False
    subject.checked = True
    db.session.commit()

    work_tasks.reset_non_bisac_nonfiction_subjects.delay().wait()
    db.session.expire_all()

    assert subject.checked is False


@pytest.mark.parametrize(
    "subject_type,identifier,fiction",
    [
        pytest.param(Subject.BISAC, "HIS027000", False, id="real_nonfiction_code"),
        pytest.param(Subject.BISAC, "HIS000000", False, id="real_nonfiction_general"),
        pytest.param(
            Subject.BISAC, "HISTORY / General", False, id="nonfiction_heading"
        ),
        pytest.param(Subject.BISAC, "INFEN000", True, id="already_scored_fiction"),
        pytest.param(Subject.BISAC, "INFEN000", None, id="already_scored_unknown"),
        pytest.param(Subject.TAG, "INFEN000", False, id="not_a_bisac_subject"),
    ],
)
def test_reset_non_bisac_nonfiction_subjects_leaves_everything_else_checked(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    subject_type: str,
    identifier: str,
    fiction: bool | None,
) -> None:
    """Legitimate nonfiction codes keep their value, and subjects outside the
    examined set -- already scored as fiction or unknown, or not BISAC-typed --
    are not touched."""
    subject = db.subject(subject_type, identifier)
    subject.fiction = fiction
    subject.checked = True
    db.session.commit()

    work_tasks.reset_non_bisac_nonfiction_subjects.delay().wait()
    db.session.expire_all()

    assert subject.checked is True


def test_reset_non_bisac_nonfiction_subjects_is_idempotent(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    """A second run finds nothing left to do and leaves the reset in place."""
    subject = db.subject(Subject.BISAC, "INFEN000")
    subject.name = "English literature"
    subject.fiction = False
    subject.checked = True
    db.session.commit()

    work_tasks.reset_non_bisac_nonfiction_subjects.delay().wait()
    db.session.expire_all()
    assert subject.checked is False

    work_tasks.reset_non_bisac_nonfiction_subjects.delay().wait()
    db.session.expire_all()
    assert subject.checked is False
