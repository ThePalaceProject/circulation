"""Re-run the null-audience reclassification now that recalculation is non-destructive.

A backlog of works with ``audience IS NULL`` remained for two reasons:

- The earlier one-time repair (startup task
  ``2026_05_12_reclassify_fb_misclassified_works``) is recorded as run on
  *dispatch*, not completion, so an interrupted Celery run would not re-fire on
  later restarts -- some works may never have been reprocessed.
- Until the default-audience fix landed, recalculating a work that gathered no
  decisive audience evidence wrote ``NULL`` back over its audience, so bulk
  reclassification was *adding* nulls rather than only repairing them.

Now that ``Work._get_default_audience()`` falls back to ``AUDIENCE_ADULT`` (and
``assign_genres`` no longer overwrites a known audience with ``NULL``), re-running
``calculate_presentation()`` on the remaining ``audience IS NULL`` works is safe and
repairs them: ``Adult`` for evidence-less works, ``Children`` for juvenile titles.
This dispatches the existing one-time Celery job to do so.

TODO: Remove this task, the earlier startup task, and
``reclassify_null_audience_works`` once it has run on all deployments (PP-4330)."""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.work import reclassify_null_audience_works
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    return reclassify_null_audience_works.s()
