#!/usr/bin/env python
"""Remove `random` sort options from Circulation Manager."""

from palace.core.model import production_session
from palace.migration_scripts import RandomSortOptionRemover

with closing(production_session()) as db:
    remover = RandomSortOptionRemover()
    remover.run(db)
