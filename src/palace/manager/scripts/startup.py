"""One-time startup task registry with auto-discovery and database tracking.

See the *Startup Tasks* section in ``README.md`` for usage details.
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

from celery.canvas import Signature
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from palace.manager.service.container import Services
from palace.manager.sqlalchemy.model.startup_task import StartupTask, StartupTaskState
from palace.manager.util.datetime_helpers import utc_now

logger = logging.getLogger(__name__)

#: Type alias for a startup task callable.
StartupTaskCallable = Callable[[Services, Session, logging.Logger], Signature | None]

#: Default location of startup task files — ``startup_tasks/`` at the
#: project root, resolved relative to this file's position in the source
#: tree.
STARTUP_TASKS_DIR = Path(__file__).parents[4] / "startup_tasks"

_TEMPLATE = '''\
"""{description}"""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
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
    tasks_dir: Path,
) -> dict[str, StartupTaskCallable]:
    """Scan *tasks_dir* for Python files that define a ``run`` callable.

    The task key is derived from the filename (without the ``.py``
    extension).  Files whose name starts with ``_`` are skipped.  Files
    that do not define a ``run`` callable are skipped with a warning.

    :param tasks_dir: Directory to scan.
    :returns: A dict mapping task key to the ``run`` callable,
        sorted by filename for deterministic ordering.
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

    return tasks


def _pending_tasks(
    session: Session,
    tasks: dict[str, StartupTaskCallable],
) -> dict[str, StartupTaskCallable]:
    """Return the subset of *tasks* not yet recorded in the database."""
    existing_keys: set[str] = set(session.scalars(select(StartupTask.key)).all())
    return {k: v for k, v in tasks.items() if k not in existing_keys}


def _record_task(session: Session, key: str, *, state: StartupTaskState) -> None:
    """Insert a :class:`StartupTask` row and flush (no commit)."""
    session.add(StartupTask(key=key, recorded_at=utc_now(), state=state))
    session.flush()


def run_startup_tasks(
    engine: Engine,
    services: Services,
    *,
    already_initialized: bool,
    tasks_dir: Path | None = None,
) -> None:
    """Discover and process one-time startup tasks.

    On an existing database, new tasks are executed.  On a fresh install
    (``already_initialized=False``), tasks are stamped as
    :attr:`~StartupTaskState.MARKED` without running — there is no
    existing data to migrate.

    Each task is executed with its own session and transaction so that a
    failure in one task does not affect others.

    :param engine: SQLAlchemy engine used to create a connection.
    :param services: The application services container, passed to each
        task alongside a database session.
    :param already_initialized: Whether the database existed before this
        startup.  When ``False`` tasks are stamped instead of run.
    :param tasks_dir: Directory to scan for task files.  Defaults to the
        project-root ``startup_tasks/`` directory.
    """
    tasks = discover_startup_tasks(tasks_dir or STARTUP_TASKS_DIR)
    if not tasks:
        logger.info("No startup tasks discovered.")
        return

    if not already_initialized:
        _stamp_tasks(engine, tasks)
        return

    _run_tasks(engine, services, tasks)


def _stamp_tasks(
    engine: Engine,
    tasks: dict[str, StartupTaskCallable],
) -> None:
    """Record all tasks as already-executed without running them."""
    logger.info(
        "Fresh database install — stamping %d startup task(s) without running.",
        len(tasks),
    )

    with Session(engine) as session, session.begin():
        pending = _pending_tasks(session, tasks)
        for key in pending:
            _record_task(session, key, state=StartupTaskState.MARKED)
            logger.info("Stamped startup task %r.", key)


def _run_tasks(
    engine: Engine,
    services: Services,
    tasks: dict[str, StartupTaskCallable],
) -> None:
    """Execute pending tasks and record them in the database."""
    logger.info("Discovered %d startup task(s).", len(tasks))

    with Session(engine) as session:
        pending = _pending_tasks(session, tasks)

    for key in tasks:
        if key not in pending:
            logger.info("Startup task %r already executed; skipping.", key)

    for key, run_fn in pending.items():
        dispatched_task_id: str | None = None
        try:
            with Session(engine) as session, session.begin():
                result = run_fn(services, session, logger)
                if isinstance(result, Signature):
                    async_result = result.apply_async()
                    dispatched_task_id = async_result.id
                _record_task(session, key, state=StartupTaskState.RUN)
        except Exception:
            logger.exception("Failed to execute startup task %r.", key)
            continue

        if dispatched_task_id is not None:
            logger.info(
                "Startup task %r dispatched Celery task (Task ID: %s).",
                key,
                dispatched_task_id,
            )

        logger.info("Executed startup task %r.", key)


# ---------------------------------------------------------------------------
# Scaffolding command: create_startup_task
# ---------------------------------------------------------------------------


_MAX_SLUG_LENGTH = 45


def _slugify(text: str) -> str:
    """Convert a description into a valid Python identifier slug.

    The result is truncated to :data:`_MAX_SLUG_LENGTH` characters,
    trimmed at the last underscore boundary to avoid cutting mid-word.
    """
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    if len(slug) > _MAX_SLUG_LENGTH:
        slug = slug[:_MAX_SLUG_LENGTH].rsplit("_", 1)[0]
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
        help="Override the YYYY_MM_DD date prefix (default: current UTC date).",
    )
    args = parser.parse_args()

    description: str = args.description
    if args.date_prefix:
        date_prefix = args.date_prefix
    else:
        now = datetime.now(tz=timezone.utc)
        date_prefix = now.strftime("%Y_%m_%d")

    slug = _slugify(description)
    if not slug:
        print("Error: description must contain at least one alphanumeric character.")
        sys.exit(1)

    filename = f"{date_prefix}_{slug}.py"
    filepath = STARTUP_TASKS_DIR / filename

    if filepath.exists():
        print(f"Error: {filepath} already exists.")
        sys.exit(1)

    content = _TEMPLATE.replace("{description}", description)
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
