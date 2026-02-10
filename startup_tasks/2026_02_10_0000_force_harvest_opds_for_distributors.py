"""Force re-harvest all OPDS for Distributors collections.

This startup task was added after streaming media support landed (PR #3015)
to ensure existing collections are re-imported with the new parsing logic.
"""

from __future__ import annotations

from celery.canvas import Signature


def startup_task_signature() -> Signature:
    """Build the Celery signature to dispatch.

    Uses a local import to avoid import-time coupling with the Celery app,
    which may not be configured when the init script first imports this package.
    """
    from palace.manager.celery.tasks.opds_for_distributors import import_all

    return import_all.si(force=True)
