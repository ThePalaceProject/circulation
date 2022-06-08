#!/usr/bin/env python
import logging
import os
import sys

from sqlalchemy import text
from sqlalchemy.orm import Session

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import production_session

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel("INFO")


def execute_migration(db: Session) -> None:
    db.execute(
        text(
            """
UPDATE configurationsettings
  SET value = 'false'
  WHERE value = 'Do not de-prioritize'
    """
        )
    )
    db.execute(
        text(
            """
UPDATE configurationsettings
  SET value = 'true'
  WHERE value = 'De-prioritize'
    """
        )
    )
    db.commit()


#
# The purpose of this migration is to fix any weird values that were inserted
# into the configurationsettings table by the Admin API. Previous versions of
# the code represented the LCP priority setting as an enum, and the Admin API
# inserted the configuration labels rather than the enum values into the database.
#


def main() -> None:
    session = production_session()
    try:
        execute_migration(session)
    finally:
        session.close()


main()
