"""
This file provides the entry point for the Celery worker. When running the Celery worker from the
command line. You can use the following command:
```
celery -A "core.celery.worker.app" worker
```

Note: This file is not used in the app directly and shouldn't be imported imported anywhere.
Its only used to provide a global app instance for the Celery worker to use.
"""

import importlib
from pathlib import Path
from typing import Any

from celery.signals import setup_logging

from core.service.container import container_instance


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
        module = f"core.celery.tasks.{task_file.stem}"
        importlib.import_module(module)


@setup_logging.connect
def celery_logger_setup(
    loglevel: int, logfile: str, format: str, colorize: bool, **kwargs: Any
) -> None:
    # Override the default Celery logger setup to use the logger configuration from the service container,
    # this will likely need to be updated so that we respect some of the Celery specific configuration options.
    ...


services = container_instance()
services.init_resources()
import_celery_tasks()
app = services.celery.app()
