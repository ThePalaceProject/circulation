"""Force re-harvest all OPDS for Distributors collections.

This startup task was added after streaming media support landed (PR #3015)
to ensure existing collections are re-imported with the new parsing logic."""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.opds_for_distributors import import_all
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    return import_all.si(force=True)
