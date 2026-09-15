"""Repair BISAC subjects stored as nonfiction because their code did not resolve.

Everything on the Palace Marketplace / Feedbooks category scheme is stored with
``type='BISAC'``, including codes that are not BISAC at all -- language and
territory categories such as ``INFEN000`` ("English literature"). Those cannot
be resolved to a canonical heading, so classification used to infer nonfiction
from the distributor's name and store ``fiction=False``. The classifier no
longer does that, which leaves the stored values stale.

Subjects are only re-examined when ``checked`` is false, so this dispatches two
steps: ``reset_non_bisac_nonfiction_subjects`` marks the stale ones unchecked,
then ``classify_unchecked_subjects`` re-scores them and recalculates their
works. The second signature is immutable so the chain does not pass the first
task's return value into it.

Doing both here matters. The reset on its own is exposed: anything reaching
``Subject.assign_to_genre`` before the re-score consumes it, and code running
the superseded rules re-stamps ``checked=True`` with the same wrong value.
Nothing errors and nothing revisits the subject afterwards, so the repair
silently did nothing, having paid for a reindex to do it. Chaining the re-score
closes that gap to seconds rather than waiting for the nightly run.

The timing works out. ``helpers/migrate.yml`` stops the scripts container --
where every Celery worker and beat run -- before migrating, and starts it again
from the new image afterwards, so the worker that picks this up is necessarily
new code.

Web containers are the remaining exposure: the deploy recycles them after the
migration step, and they can reach ``assign_to_genre`` through a presentation
recalculation. Fargate deployments are not governed by that playbook at all. A
second startup task re-applies the reset a release later, once no old code is
running anywhere.

TODO: Remove this task once it has run on all deployments (PP-5129)."""

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
    return chain(
        reset_non_bisac_nonfiction_subjects.s(),
        classify_unchecked_subjects.si(),
    )
