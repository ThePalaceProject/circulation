#!/usr/bin/env python

import logging
import sys

from customlist_export import CustomListExporter

logging.basicConfig()
logger = logging.getLogger()


def main():
    CustomListExporter.create(sys.argv[1:]).execute()


if __name__ == "__main__":
    main()
