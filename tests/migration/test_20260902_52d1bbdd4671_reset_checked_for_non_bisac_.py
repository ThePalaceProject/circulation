import pytest
from pytest_alembic import MigrationContext

from tests.migration.conftest import AlembicDatabaseFixture

REVISION = "52d1bbdd4671"


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
def test_resets_subjects_no_longer_scored_as_nonfiction(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
    identifier: str,
    name: str | None,
) -> None:
    """Subjects stored as nonfiction that the classifier no longer scores that
    way are marked unchecked, so classify_unchecked_subjects re-scores them.

    Covers both codes that are not BISAC at all and codes that merely look like
    one (FBZZZ000000), which a pattern-based predicate would wrongly accept.
    """
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    subject_id = alembic_database.subject(
        "BISAC", identifier, name=name, fiction=False, checked=True
    )

    alembic_runner.migrate_up_one()

    assert alembic_database.fetch_subject(subject_id).checked is False


@pytest.mark.parametrize(
    "subject_type,identifier,name,fiction",
    [
        pytest.param("BISAC", "HIS027000", None, False, id="real_nonfiction_code"),
        pytest.param("BISAC", "HIS000000", None, False, id="real_nonfiction_general"),
        pytest.param(
            "BISAC", "HISTORY / General", None, False, id="nonfiction_heading"
        ),
        pytest.param("BISAC", "INFEN000", None, True, id="already_scored_fiction"),
        pytest.param("BISAC", "INFEN000", None, None, id="already_scored_unknown"),
        pytest.param("tag", "INFEN000", None, False, id="not_a_bisac_subject"),
    ],
)
def test_leaves_everything_else_checked(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
    subject_type: str,
    identifier: str,
    name: str | None,
    fiction: bool | None,
) -> None:
    """Legitimate nonfiction codes keep their value, and subjects outside the
    examined set -- already scored as fiction or unknown, or not BISAC-typed --
    are not touched."""
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    subject_id = alembic_database.subject(
        subject_type, identifier, name=name, fiction=fiction, checked=True
    )

    alembic_runner.migrate_up_one()

    assert alembic_database.fetch_subject(subject_id).checked is True
