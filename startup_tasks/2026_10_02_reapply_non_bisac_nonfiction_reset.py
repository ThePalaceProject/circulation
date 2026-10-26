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

The re-score is chained rather than left to the nightly
``classify_unchecked_subjects``, for a reason that outlives the old-code race.
``assign_to_genre`` marks a subject checked *before* classifying it, and the
nightly selects works by ``Subject.checked == False``. One admin edit through
the work editor therefore flips a shared BISAC subject to checked -- storing
the right value on the subject, since the code is current -- while every other
work carrying that subject drops out of the nightly's query with its stale
genres intact and nothing left to revisit it. Chaining shrinks that window from
a day to seconds. The pass is not scoped to this reset, so it also picks up
whatever else is unchecked, but that is the same set the nightly run would
process a few hours later anyway.

This mirrors the null-audience repair, where ``2026_06_17`` re-ran what
``2026_05_12`` had dispatched a release earlier.

TODO: Remove the whole repair once this has run on all deployments (PP-5129):
this task, the release N startup task, ``reset_non_bisac_nonfiction_subjects``
with its script and bin wrapper, and
``BISACClassifier.contradicts_stored_fiction``, whose only non-test caller is
that task."""

from __future__ import annotations

import logging

from celery.canvas import Signature, chain
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.work import (
    classify_unchecked_subjects,
    reset_non_bisac_nonfiction_subjects,
)
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    # Immutable so the chain does not pass the reset's return value into the
    # re-score, which takes no arguments.
    return chain(
        reset_non_bisac_nonfiction_subjects.s(),
        classify_unchecked_subjects.si(),
    )
