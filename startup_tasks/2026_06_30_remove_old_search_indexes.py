"""Remove the orphaned v5, v6 and v7 search indexes.

The v5-v7 search schema revisions were removed once production moved to the
self-contained v8 schema. Earlier reindex migrations re-pointed the search
read/write aliases at each new index but never deleted the index they replaced,
so ``circulation-works-v5``, ``-v6`` and ``-v7`` linger in the cluster consuming
shards and disk. This removes them if they are still present, skipping any index
an alias still points at.

TODO: Remove this task once it has run on all deployments.
"""

from __future__ import annotations

import logging

from celery.canvas import Signature
from sqlalchemy.orm import Session

from palace.manager.search.service import remove_search_indices
from palace.manager.service.container import Services


def run(services: Services, session: Session, log: logging.Logger) -> Signature | None:
    remove_search_indices(services.search.service(), [5, 6, 7], log=log)
    return None
