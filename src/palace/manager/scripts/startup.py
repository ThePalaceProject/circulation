"""One-time startup task registry with auto-discovery and database tracking.

Developers register tasks by adding a Python file to the ``startup_tasks/``
directory at the project root.  Each file must define a ``startup_task_signature``
callable returning a Celery :class:`~celery.canvas.Signature`.

On each application start the :class:`StartupTaskRunner` discovers registered
tasks, checks the database for previously-queued entries, and dispatches any
new ones to Celery.

**Adding a task:**

1. Run ``create_startup_task <short_description>`` to scaffold a new file.
2. Implement ``startup_task_signature() -> Signature`` in the generated file.
3. Deploy — the init script auto-discovers and queues it.

The task key is derived automatically from the filename (e.g.
``2026_02_10_0000_force_harvest.py`` → key ``2026_02_10_0000_force_harvest``).
The module docstring serves as the human-readable description.

**Cleaning up:**

Delete the file once every environment has executed the task.  The database
row is retained as a historical record.
"""

from __future__ import annotations

import importlib.util
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

from palace.manager.sqlalchemy.model.startup_task import StartupTask
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin

#: Default location of startup task files — ``startup_tasks/`` at the
#: project root, resolved relative to this file's position in the source
#: tree.
STARTUP_TASKS_DIR = Path(__file__).parents[4] / "startup_tasks"

_TEMPLATE = '''\
"""{description}"""

from __future__ import annotations

from celery.canvas import Signature


def startup_task_signature() -> Signature:
    """Build the Celery signature to dispatch.

    Uses a local import to avoid import-time coupling with the Celery app,
    which may not be configured when the init script first imports this package.
    """
    raise NotImplementedError("TODO: return a Celery signature here")
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
) -> dict[str, Callable[[], Signature]]:
    """Scan *tasks_dir* for Python files that define a ``startup_task_signature`` callable.

    The task key is derived from the filename (without the ``.py``
    extension).  Files whose name starts with ``_`` are skipped.  Files
    that do not define a ``startup_task_signature`` callable are skipped with a
    warning.

    :param tasks_dir: Directory to scan.  Defaults to the project-root
        ``startup_tasks/`` directory.
    :returns: A dict mapping task key to the ``startup_task_signature`` callable,
        sorted by key for deterministic ordering.
    """
    logger = StartupTaskRunner.logger()

    if not tasks_dir.is_dir():
        logger.info("Startup tasks directory %s does not exist; skipping.", tasks_dir)
        return {}

    tasks: dict[str, Callable[[], Signature]] = {}

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

        create_sig = getattr(module, "startup_task_signature", None)
        if create_sig is None:
            logger.warning(
                "Startup task module %s does not define 'startup_task_signature'; skipping.",
                module_path.stem,
            )
            continue

        if not callable(create_sig):
            logger.warning(
                "Startup task module %s has a 'startup_task_signature' attribute "
                "that is not callable; skipping.",
                module_path.stem,
            )
            continue

        tasks[module_path.stem] = create_sig

    return dict(sorted(tasks.items()))


class StartupTaskRunner(LoggerMixin):
    """Discover and dispatch unexecuted startup tasks.

    Each task is committed independently so that a failure in one task does
    not prevent others from being recorded and queued.
    """

    def run(self, engine: Engine, *, stamp_only: bool = False) -> None:
        """Discover tasks, check database, and queue or stamp new ones.

        :param engine: SQLAlchemy engine used to create a session.
        :param stamp_only: If ``True``, record all discovered tasks as
            already-queued **without** dispatching them to Celery.  This is
            used on fresh database installs where there is no existing data
            to migrate — analogous to ``alembic stamp head``.
        """
        tasks = discover_startup_tasks()
        if not tasks:
            self.log.info("No startup tasks discovered.")
            return

        if stamp_only:
            self.log.info(
                "Fresh database install — stamping %d startup task(s) "
                "without queuing.",
                len(tasks),
            )
        else:
            self.log.info("Discovered %d startup task(s).", len(tasks))

        with Session(engine) as session:
            existing_keys: set[str] = set(
                session.scalars(select(StartupTask.key)).all()
            )

        for key, startup_task_signature in tasks.items():
            if key in existing_keys:
                self.log.info("Startup task %r already queued; skipping.", key)
                continue

            if not stamp_only:
                try:
                    sig = startup_task_signature()
                    sig.apply_async()
                except Exception:
                    self.log.exception("Failed to queue startup task %r.", key)
                    continue

            with Session(engine) as session:
                row = StartupTask(
                    key=key,
                    queued_at=utc_now(),
                    run=not stamp_only,
                )
                session.add(row)
                session.commit()

            if stamp_only:
                self.log.info("Stamped startup task %r.", key)
            else:
                self.log.info("Queued startup task %r.", key)


# ---------------------------------------------------------------------------
# Scaffolding command: bin/create_startup_task
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
    print(f"Created {filepath.relative_to(Path.cwd())}")
    print(f"Task key will be: {filepath.stem}")
    print()
    print("Next steps:")
    print("  1. Implement startup_task_signature() in the generated file.")
    print("  2. Commit and deploy.")
