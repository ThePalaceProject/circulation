from palace.manager.celery.tasks.saml import update_saml_federation_idps_metadata
from palace.manager.scripts.base import Script


class UpdateSamlMetadata(Script):
    """A convenient script for manually kicking an asynchronous saml update task."""

    def do_run(self, *args, **kwargs):
        update_saml_federation_idps_metadata.delay()
        self.log.info(
            "update_saml_federation_idps_metadata task has been queued for execution. See celery logs "
            "for more info about task execution"
        )
