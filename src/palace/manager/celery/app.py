"""
This file provides the entry point for the Celery cli scripts. When running the Celery worker from the
command line. You can use the following command:
```
celery -A "palace.manager.celery.app" worker
```

Note: This file is not used in the app directly and shouldn't be imported anywhere.
Its only used to provide a global app instance for the Celery cli to use.
"""

import logging
from logging.handlers import WatchedFileHandler
from typing import Any

from celery.signals import setup_logging

from palace.manager.service.container import container_instance


@setup_logging.connect
def celery_logger_setup(loglevel: int, logfile: str | None, **kwargs: Any) -> None:
    services = container_instance()
    level = services.logging.config.level()  # type: ignore[attr-defined]
    container_level = level.levelno if level is not None else None
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
import palace.manager.celery.tasks  # noqa: autoflake

app = services.celery.app()
