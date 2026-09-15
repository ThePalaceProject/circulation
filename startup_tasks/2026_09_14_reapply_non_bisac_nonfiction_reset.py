"""Re-apply the non-BISAC nonfiction reset now that no old code is running.

Release N repaired these subjects: startup task
``2026_09_14_reclassify_non_bisac_nonfiction_subjects`` reset ``checked=False``
on BISAC subjects stored as nonfiction because an unresolvable code fell
through the ruleset catch-all, and chained the re-score behind it.

That repair is exposed for as long as any old code is still running. Anything
reaching Subject.assign_to_genre before the re-score lands consumes the reset,
and code running the superseded rules re-stamps checked=True with the same
wrong value -- silently, and with nothing to revisit the subject afterwards.
The deploy stops the Celery workers before the migrate step, so they are safe,
but the web containers are recycled after it and Fargate deployments are not
governed by that playbook at all.

This task exists to close that off. Running it a release later means no old
code is live anywhere, so the reset it applies cannot be consumed under the old
rules. It is idempotent: if the release N repair took, this finds nothing to do
and the run is a no-op.

Re-scoring is left to the nightly classify_unchecked_subjects. There is no need
to dispatch it here, because by now there is no old code to lose the reset to.

This is the same shape as the null-audience repair, where startup task
2026_06_17 re-ran what 2026_05_12 had dispatched a release earlier.

TODO: Remove this task, the release N startup task, and
reset_non_bisac_nonfiction_subjects once this has run on all deployments
(PP-5129)."""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.work import reset_non_bisac_nonfiction_subjects
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    return reset_non_bisac_nonfiction_subjects.s()
