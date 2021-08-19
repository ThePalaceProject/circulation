#!/usr/bin/env python
"""Update the circulation manager server with new books from
OPDS 2.x + ODL collections."""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from webpub_manifest_parser.odl import ODLFeedParserFactory

from api.odl2 import ODL2API, ODL2Importer, ODL2ImportMonitor
from core.opds2_import import RWPMManifestParser
from core.scripts import OPDSImportScript

import_script = OPDSImportScript(
    importer_class=ODL2Importer,
    monitor_class=ODL2ImportMonitor,
    protocol=ODL2API.NAME,
    parser=RWPMManifestParser(ODLFeedParserFactory()),
)

import_script.run()
