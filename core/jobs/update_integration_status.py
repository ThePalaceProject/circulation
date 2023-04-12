import datetime

from sqlalchemy import func

from core.model import ExternalIntegration, ExternalIntegrationError
from core.scripts import Script
from core.util.datetime_helpers import utc_now


class UpdateIntegrationStatusScript(Script):
    """Script to update the external integration status fields
    based on any errors logged in the db
    """

    # 10 errors in the last 30 minutes
    ERROR_WINDOW_DELTA = datetime.timedelta(minutes=30)
    ERROR_WINDOW_COUNT = 10

    def do_run(self):
        self._do_run_patron_auth()
        self._db.commit()

    def _do_run_patron_auth(self):
        now = utc_now()

        # All intergations of the patron auth type
        auth_ids = (
            self._db.query(ExternalIntegration)
            .filter(ExternalIntegration.goal == ExternalIntegration.PATRON_AUTH_GOAL)
            .values("id")
        )
        auth_ids = [_id[0] for _id in auth_ids]

        # Which auth's have the required number of errors in the given window
        should_block_ids = (
            self._db.query(ExternalIntegrationError)
            .filter(
                ExternalIntegrationError.time >= now - self.ERROR_WINDOW_DELTA,
                ExternalIntegrationError.external_integration_id.in_(auth_ids),
            )
            .group_by(ExternalIntegrationError.external_integration_id)
            .having(func.count(ExternalIntegrationError.id) >= self.ERROR_WINDOW_COUNT)
            .values(
                ExternalIntegrationError.external_integration_id,
                func.count(ExternalIntegrationError.id),
            )
        )
        should_block_ids = [_id[0] for _id in should_block_ids]

        # Update all others to GREEN
        updated = (
            self._db.query(ExternalIntegration)
            .filter(
                ExternalIntegration.id.notin_(should_block_ids),
                ExternalIntegration.status == ExternalIntegration.RED,
            )
            .update(
                {
                    ExternalIntegration.status: ExternalIntegration.GREEN,
                    ExternalIntegration.last_status_update: now,
                },
                synchronize_session=False,
            )
        )
        self.log.info(f"Updated {updated} integrations to GREEN")

        # The filtered integrations should all be set to RED
        integrations = (
            self._db.query(ExternalIntegration)
            .filter(ExternalIntegration.id.in_(should_block_ids))
            .all()
        )
        for integration in integrations:
            self.log.info(f"Setting integration status: '{integration.name}' = RED")
            integration.status = ExternalIntegration.RED
            integration.last_status_update = now
