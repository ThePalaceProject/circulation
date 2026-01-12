from typing import Any

from palace.manager.celery.tasks.saml import update_saml_federation_idps_metadata
from palace.manager.scripts.base import Script


class UpdateSamlMetadata(Script):
    """A convenient script for manually kicking off an asynchronous saml update task."""

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        update_saml_federation_idps_metadata.delay()
        self.log.info(
            'The "update_saml_federation_idps_metadata" task has been queued for execution. See the celery logs '
            "for details about task execution."
        )
