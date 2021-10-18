#!/usr/bin/env python
"""Make sure every CustomListEntry has a LicensePool set.
"""
import logging
import os
import sys
from pdb import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))


from core.model import CustomListEntry, production_session

_db = production_session()

qu = _db.query(CustomListEntry).filter(CustomListEntry.license_pool == None)
print("Fixing %d custom list entries with no licensepool." % qu.count())

for cle in qu:
    cle.set_license_pool()
