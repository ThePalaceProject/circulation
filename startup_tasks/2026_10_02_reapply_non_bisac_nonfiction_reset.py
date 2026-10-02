"""Re-apply the non-BISAC nonfiction reset now that no old code is running.

Startup task ``2026_09_14_reclassify_non_bisac_nonfiction_subjects`` repaired
these subjects a release ago, but its reset was exposed to any old code still
live: hosting-playbook's ``helpers/migrate.yml`` recycles the web containers
after the migrate step, and does not govern Fargate deployments at all. Old
code reaching ``Subject.assign_to_genre`` consumes the reset and re-stamps
``checked=True`` with the same wrong value; see that task's docstring for the
detail. A release later nothing old is running anywhere, so the same reset is
safe to apply again.

It recomputes which subjects the classifier disagrees with rather than
replaying a stored list, so if the first repair took this is a no-op.
Re-scoring is left to the nightly ``classify_unchecked_subjects``; the release
N task chained it only because it was racing old code.

This mirrors the null-audience repair, where ``2026_06_17`` re-ran what
``2026_05_12`` had dispatched a release earlier.

TODO: Remove the whole repair once this has run on all deployments (PP-5129):
this task, the release N startup task, ``reset_non_bisac_nonfiction_subjects``
with its script and bin wrapper, and
``BISACClassifier.contradicts_stored_fiction``, whose only non-test caller is
that task."""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.work import reset_non_bisac_nonfiction_subjects
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    return reset_non_bisac_nonfiction_subjects.s()
