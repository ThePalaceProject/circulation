"""Clear presentation readiness for works that have no delivery mechanism.

Presentation readiness now requires a license pool with at least one delivery
mechanism. Works that predate that rule are still marked ready, so the search
index keeps matching them and the feed layer keeps dropping them -- which is
what made broad filters return short or empty pages (PP-5434). This dispatches
the one-time Celery job that recomputes the flag for them.

TODO: Remove this task and ``reset_undeliverable_works`` once it has run on all
deployments."""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.work import reset_undeliverable_works
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    return reset_undeliverable_works.s()
