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
    ],
)
def test_resets_non_canonical_nonfiction_subjects(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
    identifier: str,
    name: str | None,
) -> None:
    """Codes that are not BISAC at all, holding a fabricated fiction=False, are
    marked unchecked so classify_unchecked_subjects re-scores them."""
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    subject_id = alembic_database.subject(
        "BISAC", identifier, name=name, fiction=False, checked=True
    )

    alembic_runner.migrate_up_one()

    assert alembic_database.fetch_subject(subject_id).checked is False


@pytest.mark.parametrize(
    "subject_type,identifier,fiction",
    [
        pytest.param("BISAC", "FIC014000", False, id="canonical_code"),
        pytest.param("BISAC", "FBFIC014000", False, id="canonical_fb_prefixed"),
        pytest.param("BISAC", "FBJUV000000N", False, id="canonical_fb_and_n"),
        pytest.param("BISAC", "HIS027000", False, id="canonical_real_nonfiction"),
        pytest.param("BISAC", "INFEN000", True, id="non_canonical_already_fiction"),
        pytest.param("BISAC", "INFEN000", None, id="non_canonical_already_null"),
        pytest.param("tag", "INFEN000", False, id="not_a_bisac_subject"),
    ],
)
def test_leaves_everything_else_checked(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
    subject_type: str,
    identifier: str,
    fiction: bool | None,
) -> None:
    """The reset is narrow: canonical BISAC codes, non-canonical codes that are
    not voting nonfiction, and non-BISAC subject types are all left alone."""
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    subject_id = alembic_database.subject(
        subject_type, identifier, fiction=fiction, checked=True
    )

    alembic_runner.migrate_up_one()

    assert alembic_database.fetch_subject(subject_id).checked is True
