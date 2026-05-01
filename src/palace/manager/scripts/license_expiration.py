from typing import Any

from palace.manager.celery.tasks.license_expiration import expire_licenses
from palace.manager.scripts.base import Script


class ExpireLicensesScript(Script):
    """Manually kick off the expire_licenses Celery task."""

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        expire_licenses.delay()
        self.log.info(
            'The "expire_licenses" task has been queued for execution. See the celery logs '
            "for details about task execution."
        )
