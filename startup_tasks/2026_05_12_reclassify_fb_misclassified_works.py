"""Reclassify works whose audience was reset by the FB BISAC mis-classification repair.

The migration d856ff4dbefb set audience=NULL for works that had audience='Adult' but
were linked to FB-prefixed BISAC subjects now correctly classified as Children/Young
Adult. This task dispatches the one-time Celery job that re-runs calculate_presentation()
on those works."""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.work import reclassify_null_audience_works
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    return reclassify_null_audience_works.s()
