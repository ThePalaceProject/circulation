"""Force re-harvest all OPDS for Distributors collections.

This startup task was added after streaming media support landed (PR #3015)
to ensure existing collections are re-imported with the new parsing logic.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from palace.manager.service.container import Services


def run(services: Services, session: Session) -> None:
    # Local import to avoid import-time coupling with the Celery app,
    # which may not be configured when the init script first imports
    # this module.
    from palace.manager.celery.tasks.opds_for_distributors import import_all

    import_all.si(force=True).apply_async()
