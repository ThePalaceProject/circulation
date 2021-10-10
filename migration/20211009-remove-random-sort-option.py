#!/usr/bin/env python
"""Remove `random` sort options from Circulation Manager."""

import os
import sys

from contextlib2 import closing
from migartion_scripts import RandomSortOptionRemover

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import production_session

with closing(production_session()) as db:
    remover = RandomSortOptionRemover()
    remover.run(db)
