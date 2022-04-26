#!/usr/bin/env python

import logging
import sys

from customlists.customlist_import import CustomListImporter

logging.basicConfig()
logger = logging.getLogger()


def main():
    CustomListImporter.create(sys.argv[1:]).execute()


if __name__ == "__main__":
    main()
