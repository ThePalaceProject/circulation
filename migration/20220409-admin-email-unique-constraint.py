import logging
import os
import sys

from contextlib2 import closing
from sqlalchemy import Index, func
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import Session

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import production_session
from core.model.admin import Admin

INDEX_NAME = "ix_admin_upper_email_unique"


def create_unique_email_constraint(db: Session):
    """Create a unique index on UPPER(Admin.email)
    This is to prevent duplicates for case-insensitive login for admins"""

    index = Index(INDEX_NAME, func.upper(Admin.email), unique=True)
    engine = db.get_bind()

    try:
        index.create(engine)
        db.commit()
    except (ProgrammingError, IntegrityError) as ex:
        logging.getLogger().error(f"Could not create index {INDEX_NAME}")
        logging.getLogger().error(ex.args)
        return False

    logging.getLogger().info(f"Successfully added index {INDEX_NAME}")

    return True


if __name__ == "__main__":
    with closing(production_session()) as db:
        create_unique_email_constraint(db)
