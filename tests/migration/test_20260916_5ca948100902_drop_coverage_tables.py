from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext as RuntimeMigrationContext
from pytest_alembic import MigrationContext
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

REVISION = "5ca948100902"
DOWN_REVISION = "58ebd34c5092"


def _load_revision() -> ModuleType:
    """Import this revision as a standalone module.

    Revision files are not importable as a package, so load it by path. The
    module is a private copy, so rebinding its ``op`` below cannot affect the
    real alembic proxy used elsewhere.
    """
    versions = Path(__file__).parents[2] / "alembic" / "versions"
    (path,) = versions.glob(f"*_{REVISION}_*.py")
    spec = importlib.util.spec_from_file_location(f"revision_{REVISION}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _coverage_status_exists(engine: Engine) -> bool:
    with engine.begin() as connection:
        # Raw SQL rather than Inspector.get_enums(), which exists only on the
        # Postgres inspector and so is not on the type mypy sees here.
        return (
            connection.execute(
                text("SELECT count(*) FROM pg_type WHERE typname = 'coverage_status'")
            ).scalar_one()
            == 1
        )


def test_drop_coverage_tables(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
) -> None:
    """The migration drops coveragerecords and equivalentscoveragerecords.

    Stepping the migration down recreates both tables (and the shared
    ``coverage_status`` enum); stepping it back up drops them again while
    leaving the unrelated ``timestamps`` table in place.
    """
    alembic_runner.migrate_down_to(REVISION)
    # Step down once more so the tables exist again.
    alembic_runner.migrate_down_one()
    assert alembic_runner.current == DOWN_REVISION

    tables = set(inspect(alembic_engine).get_table_names())
    assert "coveragerecords" in tables
    assert "equivalentscoveragerecords" in tables
    assert _coverage_status_exists(alembic_engine)

    # Apply the drop.
    alembic_runner.migrate_up_one()
    assert alembic_runner.current == REVISION

    tables = set(inspect(alembic_engine).get_table_names())
    assert "coveragerecords" not in tables
    assert "equivalentscoveragerecords" not in tables
    # The enum was shared by the two tables just dropped and goes with them;
    # nothing else asserts this, so losing the drop would otherwise be silent.
    assert not _coverage_status_exists(alembic_engine)
    # The unrelated timestamps table is left in place.
    assert "timestamps" in tables


def test_upgrade_does_not_leak_lock_timeout(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
) -> None:
    """upgrade() leaves lock_timeout exactly as it found it.

    The drops run under a short ``lock_timeout``, set with ``SET LOCAL``, which
    lasts to the end of the *transaction* rather than the end of this revision.
    ``alembic/env.py`` runs every pending revision inside a single
    ``context.begin_transaction()``, so without the explicit reset the timeout
    would silently apply to every later revision in the same upgrade. This
    revision is currently at the head, so the leak is latent -- it would bite
    whichever revision is added next.

    As in the 58ebd34c5092 test, driving the migration through
    ``alembic_runner`` could not catch this: it commits between revisions,
    which discards the setting regardless of what the migration did. So run
    ``upgrade()`` against a transaction we hold open ourselves, which is the
    situation env.py actually creates.
    """
    # Step back over this revision so its downgrade recreates the tables the
    # upgrade below drops.
    alembic_runner.migrate_down_one()
    assert alembic_runner.current == DOWN_REVISION

    revision = _load_revision()

    with alembic_engine.begin() as connection:
        before = connection.execute(text("SHOW lock_timeout")).scalar_one()

        # setattr, not plain assignment: the module is typed as ModuleType, so
        # mypy does not know about the ``op`` its revision file imports.
        setattr(
            revision, "op", Operations(RuntimeMigrationContext.configure(connection))
        )
        revision.upgrade()

        after = connection.execute(text("SHOW lock_timeout")).scalar_one()

    assert after == before
