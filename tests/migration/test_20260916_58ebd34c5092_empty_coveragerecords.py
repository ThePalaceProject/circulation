from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext as RuntimeMigrationContext
from pytest_alembic import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Engine

REVISION = "58ebd34c5092"


def _coveragerecord_count(engine: Engine) -> int:
    with engine.begin() as connection:
        return connection.execute(
            text("SELECT count(*) FROM coveragerecords")
        ).scalar_one()


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


def test_empties_coveragerecords(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
) -> None:
    """The migration removes every row from coveragerecords.

    Every column except the primary key is nullable, so a bare insert is enough
    to stand in for a leftover row written by the retired CoverageProvider.
    """
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO coveragerecords (operation) VALUES ('import')")
        )
    assert _coveragerecord_count(alembic_engine) == 1

    alembic_runner.migrate_up_one()
    assert alembic_runner.current == REVISION

    assert _coveragerecord_count(alembic_engine) == 0

    # The table itself survives this release -- it is dropped in the next one.
    with alembic_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO coveragerecords (operation) VALUES ('import')")
        )
    assert _coveragerecord_count(alembic_engine) == 1


def test_upgrade_does_not_leak_lock_timeout(alembic_engine: Engine) -> None:
    """upgrade() leaves lock_timeout exactly as it found it.

    The TRUNCATE runs under a short ``lock_timeout``, set with ``SET LOCAL``,
    which lasts to the end of the *transaction* rather than the end of this
    revision. ``alembic/env.py`` runs every pending revision inside a single
    ``context.begin_transaction()``, so without an explicit reset the timeout
    would silently apply to every later revision in the same upgrade.

    Driving the migration through ``alembic_runner`` could not catch that: it
    commits between revisions, which discards the setting regardless of what
    the migration did. So run ``upgrade()`` against a transaction we hold open
    ourselves, which is the situation env.py actually creates.
    """
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
