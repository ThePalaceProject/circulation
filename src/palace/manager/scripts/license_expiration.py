from typing import Any

from palace.manager.celery.tasks.license_expiration import update_expired_licenses
from palace.manager.scripts.base import Script


class UpdateExpiredLicensesScript(Script):
    """Manually kick off the update_expired_licenses Celery task."""

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        update_expired_licenses.delay()
        self.log.info(
            'The "update_expired_licenses" task has been queued for execution. See the celery logs '
            "for details about task execution."
        )
