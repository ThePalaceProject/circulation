#!/usr/bin/env python
import logging

from contextlib2 import closing
from sqlalchemy import Index, func
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from palace.core.model import production_session
from palace.core.model.admin import Admin

INDEX_NAME = "ix_admin_upper_email_unique"


def create_unique_email_constraint(db: Session):
    """Create a unique index on UPPER(Admin.email)
    This is to prevent duplicates for case-insensitive login for admins"""

    index = Index(INDEX_NAME, func.upper(Admin.email), unique=True)
    engine = db.get_bind()

    try:
        index.create(engine)
        db.commit()
    except ProgrammingError as ex:
        logging.getLogger().error(f"Could not create index {INDEX_NAME}")
        logging.getLogger().error(ex.args)
        return False

    logging.getLogger().info(f"Successfully added index {INDEX_NAME}")

    return True


if __name__ == "__main__":
    with closing(production_session()) as db:
        create_unique_email_constraint(db)
