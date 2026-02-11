"""One-time startup task registry with auto-discovery and database tracking.

Developers register tasks by adding a Python file to the ``startup_tasks/``
directory at the project root.  Each file must define a ``run`` function
with the signature::

    def run(services: Services, session: Session) -> None: ...

The function receives the application's services container and a database
session, giving it access to Redis, search, Celery dispatch, and any other
service it needs.

On each application start the :class:`StartupTaskRunner` discovers registered
tasks, checks the database for previously-executed entries, and runs any new
ones.

**Adding a task:**

1. Run ``create_startup_task <short_description>`` to scaffold a new file.
2. Implement ``run()`` in the generated file.
3. Deploy — the init script auto-discovers and executes it.

The task key is derived automatically from the filename (e.g.
``2026_02_10_0000_force_harvest.py`` → key ``2026_02_10_0000_force_harvest``).
The module docstring serves as the human-readable description.

**Cleaning up:**

Delete the file once every environment has executed the task.  The database
row is retained as a historical record.
"""

from __future__ import annotations

import importlib.util
import logging
import re
import sys
from argparse import ArgumentParser
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

from sqlalchemy import select
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from palace.manager.service.container import Services
from palace.manager.sqlalchemy.model.startup_task import StartupTask
from palace.manager.util.datetime_helpers import utc_now

logger = logging.getLogger(__name__)

#: Type alias for a startup task callable.
StartupTaskCallable = Callable[[Services, Session], None]

#: Default location of startup task files — ``startup_tasks/`` at the
#: project root, resolved relative to this file's position in the source
#: tree.
STARTUP_TASKS_DIR = Path(__file__).parents[4] / "startup_tasks"

_TEMPLATE = '''\
"""{description}"""

from __future__ import annotations

from sqlalchemy.orm import Session

from palace.manager.service.container import Services


def run(services: Services, session: Session) -> None:
    raise NotImplementedError("TODO: implement this startup task")
'''


def _load_module_from_file(name: str, path: Path) -> ModuleType:
    """Load a Python module from a file path.

    :param name: Module name to assign.
    :param path: Absolute path to the ``.py`` file.
    :raises ImportError: If the module cannot be loaded.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def discover_startup_tasks(
    tasks_dir: Path = STARTUP_TASKS_DIR,
) -> dict[str, StartupTaskCallable]:
    """Scan *tasks_dir* for Python files that define a ``run`` callable.

    The task key is derived from the filename (without the ``.py``
    extension).  Files whose name starts with ``_`` are skipped.  Files
    that do not define a ``run`` callable are skipped with a warning.

    :param tasks_dir: Directory to scan.  Defaults to the project-root
        ``startup_tasks/`` directory.
    :returns: A dict mapping task key to the ``run`` callable,
        sorted by key for deterministic ordering.
    """
    if not tasks_dir.is_dir():
        logger.info("Startup tasks directory %s does not exist; skipping.", tasks_dir)
        return {}

    tasks: dict[str, StartupTaskCallable] = {}

    for module_path in sorted(tasks_dir.glob("*.py")):
        if module_path.stem.startswith("_"):
            continue

        try:
            module = _load_module_from_file(module_path.stem, module_path)
        except Exception:
            logger.exception(
                "Failed to import startup task module %s.", module_path.stem
            )
            continue

        run_fn = getattr(module, "run", None)
        if run_fn is None:
            logger.warning(
                "Startup task module %s does not define 'run'; skipping.",
                module_path.stem,
            )
            continue

        if not callable(run_fn):
            logger.warning(
                "Startup task module %s has a 'run' attribute "
                "that is not callable; skipping.",
                module_path.stem,
            )
            continue

        tasks[module_path.stem] = run_fn

    return dict(sorted(tasks.items()))


def _pending_tasks(
    session: Session,
    tasks: dict[str, StartupTaskCallable],
) -> dict[str, StartupTaskCallable]:
    """Return the subset of *tasks* not yet recorded in the database."""
    existing_keys: set[str] = set(session.scalars(select(StartupTask.key)).all())
    return {k: v for k, v in tasks.items() if k not in existing_keys}


def _record_task(session: Session, key: str, *, run: bool) -> None:
    """Insert a :class:`StartupTask` row and flush (no commit)."""
    session.add(StartupTask(key=key, queued_at=utc_now(), run=run))
    session.flush()


def run_startup_tasks(
    connection: Connection,
    services: Services,
) -> None:
    """Discover tasks, check the database, and execute new ones.

    The function shares the caller's ``connection`` (and its transaction)
    so that it can see schema created in the same transaction — critical
    on a fresh install where the ``startup_tasks`` table has not yet been
    committed.

    Each task is executed inside a **savepoint** (``session.begin_nested()``)
    so that a failure in one task rolls back only its own changes without
    affecting other tasks or the outer transaction.

    :param connection: SQLAlchemy connection to bind sessions to.
    :param services: The application services container, passed to each
        task alongside a database session.
    """
    tasks = discover_startup_tasks()
    if not tasks:
        logger.info("No startup tasks discovered.")
        return

    logger.info("Discovered %d startup task(s).", len(tasks))

    session = Session(bind=connection)
    pending = _pending_tasks(session, tasks)

    for key in tasks:
        if key not in pending:
            logger.info("Startup task %r already executed; skipping.", key)

    for key, run_fn in pending.items():
        try:
            with session.begin_nested():
                run_fn(services, session)
        except Exception:
            logger.exception("Failed to execute startup task %r.", key)
            continue

        _record_task(session, key, run=True)
        logger.info("Executed startup task %r.", key)


def stamp_startup_tasks(connection: Connection) -> None:
    """Record all discovered tasks as already-executed **without** running them.

    This is used on fresh database installs where there is no existing data
    to migrate — analogous to ``alembic stamp head``.

    :param connection: SQLAlchemy connection to bind sessions to.
    """
    tasks = discover_startup_tasks()
    if not tasks:
        logger.info("No startup tasks discovered.")
        return

    logger.info(
        "Fresh database install — stamping %d startup task(s) without running.",
        len(tasks),
    )

    session = Session(bind=connection)
    pending = _pending_tasks(session, tasks)

    for key in pending:
        _record_task(session, key, run=False)
        logger.info("Stamped startup task %r.", key)


# ---------------------------------------------------------------------------
# Scaffolding command: create_startup_task
# ---------------------------------------------------------------------------


def _slugify(text: str) -> str:
    """Convert a description into a valid Python identifier slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug


def create_startup_task() -> None:
    """CLI entry point for scaffolding a new startup task file."""
    parser = ArgumentParser(
        description="Create a new startup task file.",
    )
    parser.add_argument(
        "description",
        help='Short description, e.g. "force harvest opds for distributors"',
    )
    parser.add_argument(
        "--date-prefix",
        default=None,
        help="Override the YYYY_MM_DD_HHMM date prefix (default: current UTC time).",
    )
    args = parser.parse_args()

    description: str = args.description
    if args.date_prefix:
        date_prefix = args.date_prefix
    else:
        now = datetime.now(tz=timezone.utc)
        date_prefix = now.strftime("%Y_%m_%d_%H%M")

    slug = _slugify(description)
    if not slug:
        print("Error: description must contain at least one alphanumeric character.")
        sys.exit(1)

    filename = f"{date_prefix}_{slug}.py"
    filepath = STARTUP_TASKS_DIR / filename

    if filepath.exists():
        print(f"Error: {filepath} already exists.")
        sys.exit(1)

    content = _TEMPLATE.format(description=description)
    filepath.write_text(content)
    try:
        display_path = filepath.relative_to(Path.cwd())
    except ValueError:
        display_path = filepath
    print(f"Created {display_path}")
    print(f"Task key will be: {filepath.stem}")
    print()
    print("Next steps:")
    print("  1. Implement run() in the generated file.")
    print("  2. Commit and deploy.")
