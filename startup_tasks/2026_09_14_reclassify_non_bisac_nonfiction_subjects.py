"""Re-score the subjects that migration 52d1bbdd4671 marked unchecked.

That migration resets ``checked=False`` on BISAC subjects stored as nonfiction
because an unresolvable code fell through the ruleset catch-all. It only resets;
``classify_unchecked_subjects`` is what re-scores them and recalculates their
works, and left to itself that does not happen until the nightly run.

The gap between the two is where the repair is exposed. Anything that reaches
``Subject.assign_to_genre`` in the meantime consumes the reset -- and if it is
running the superseded rules, it re-stamps ``checked=True`` with the same wrong
value. Nothing errors and nothing revisits the subject afterwards, so the repair
silently did nothing, having paid for a reindex to do it.

Dispatching here closes that gap to seconds. This runs from the migrate
container immediately after the migration, at a point in the deploy where the
Celery workers have been stopped and will come back on the new image, so the
task is picked up by new code.

The remaining exposure is the web containers, which the deploy recycles after
the migration and which can reach ``assign_to_genre`` through a presentation
recalculation. A later startup task re-applies the reset once no old code is
running anywhere.

TODO: Remove this task once it has run on all deployments (PP-5129)."""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.work import classify_unchecked_subjects
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    return classify_unchecked_subjects.s()
