"""
This file provides the entry point for the Celery cli scripts. When running the Celery worker from the
command line. You can use the following command:
```
celery -A "palace.manager.celery.app" worker
```

Note: This file is not used in the app directly and shouldn't be imported anywhere.
Its only used to provide a global app instance for the Celery cli to use.
"""

import importlib
import logging
from logging.handlers import WatchedFileHandler
from pathlib import Path
from typing import Any

from celery.signals import setup_logging

from palace.manager.service.container import container_instance


def import_celery_tasks() -> None:
    """
    Import all the Celery tasks from the tasks module.

    This automatically imports all the tasks from the tasks module so that they are registered
    with the worker when it starts up.

    This makes the assumption that all of our Celery tasks will be in the `tasks` module.
    """
    tasks_path = Path(__file__).parent / "tasks"
    for task_file in tasks_path.glob("*.py"):
        if task_file.stem == "__init__":
            continue
        module = f"palace.manager.celery.tasks.{task_file.stem}"
        importlib.import_module(module)


@setup_logging.connect
def celery_logger_setup(loglevel: int, logfile: str | None, **kwargs: Any) -> None:
    level = services.logging.config.level()  # type: ignore[attr-defined]
    container_level = level.levelno if level else None
    root_logger = logging.getLogger()

    # If celery requested a lower log level, then we update the root logger to use the lower level.
    if container_level is None or loglevel < container_level:
        root_logger.setLevel(loglevel)

    # If celery requested a log file, then we update the root logger to also log to the file.
    if logfile:
        handler = WatchedFileHandler(logfile, encoding="utf-8")
        handler.setFormatter(services.logging.json_formatter())
        root_logger.addHandler(handler)


services = container_instance()
services.init_resources()
import_celery_tasks()
app = services.celery.app()
