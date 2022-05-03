#!/usr/bin/env python
import logging
import os
import sys
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import production_session

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel("INFO")


def delete_key(db: Session, integration: int, key: str) -> None:
    delete_key = text(
        """
DELETE FROM configurationsettings
WHERE key = :key
AND external_integration_id = :id
"""
    )
    db.execute(delete_key, {"id": f"{integration}", "key": key})


def rename_key(db: Session, integration: int, old_key: str, new_key: str) -> None:
    rename_key = text(
        """
UPDATE configurationsettings
SET key = :new_key
WHERE key = :old_key
AND external_integration_id = :id
    """
    )
    db.execute(
        rename_key, {"id": f"{integration}", "old_key": old_key, "new_key": new_key}
    )


def update_key_value(db: Session, integration: int, old_key: str, new_key: str) -> int:
    # Try to retrieve the values of any old or new keys.
    # Note that the CM's configuration complicates things by
    # having keys that may be present but with null values.
    select_key = text(
        """
SELECT c.value FROM configurationsettings AS c
  WHERE c.key = :key
    AND c.external_integration_id = :id
    """
    )
    result_old_key = db.execute(select_key, {"id": f"{integration}", "key": old_key})

    existing_old_key_present: bool = False
    existing_old_key: Optional[str] = None
    if result_old_key.rowcount > 0:
        existing_old_key_present = True
        existing_old_key = result_old_key.fetchone()[0]

    result_new_key = db.execute(select_key, {"id": f"{integration}", "key": new_key})

    existing_new_key_present: bool = False
    existing_new_key: Optional[str] = None
    if result_new_key.rowcount > 0:
        existing_new_key_present = True
        existing_new_key = result_new_key.fetchone()[0]

    if existing_old_key_present:
        logger.info(f"Discovered old key '{old_key}' -> '{existing_old_key}'")
    if existing_new_key_present:
        logger.info(f"Discovered new key '{new_key}' -> '{existing_new_key}'")

    # If keys have a non-null value, something is seriously wrong!
    if not existing_old_key and not existing_new_key:
        raise RuntimeError(
            f"External integration {integration} has neither a '{old_key}' or a '{new_key}'"
        )

    # If both the old key and the new key exist, then keep whichever one is non-null,
    # preferring the new key to the old key.
    if existing_old_key_present and existing_new_key_present:
        if existing_new_key:
            logger.info(f"Deleting old key '{old_key}'")
            delete_key(db=db, integration=integration, key=old_key)
        else:
            assert existing_old_key
            rename_key(db=db, integration=integration, old_key=old_key, new_key=new_key)
        return 1

    # Otherwise, if only the old key exists, rename it to the new key.
    if existing_old_key_present:
        assert not existing_new_key
        assert not existing_new_key_present
        assert existing_old_key

        logger.info(f"Renaming old key '{old_key}' -> new key '{new_key}'")
        rename_key(db=db, integration=integration, old_key=old_key, new_key=new_key)
        return 1

    # The old key didn't exist, the new key exists, so there's nothing to do.
    assert existing_new_key
    assert existing_new_key_present
    assert not existing_old_key
    assert not existing_old_key_present

    logger.info(
        f"New key '{new_key}' already exists, and old key '{old_key}' does not; nothing to do!"
    )
    return 0


def update_configuration(db: Session, integration: int) -> None:
    updated = 0
    updated += update_key_value(
        db=db,
        integration=integration,
        old_key="username",
        new_key="overdrive_client_key",
    )
    updated += update_key_value(
        db=db,
        integration=integration,
        old_key="password",
        new_key="overdrive_client_secret",
    )
    updated += update_key_value(
        db=db,
        integration=integration,
        old_key="website_id",
        new_key="overdrive_website_id",
    )
    updated += update_key_value(
        db=db,
        integration=integration,
        old_key="server_nickname",
        new_key="overdrive_server_nickname",
    )

    if updated > 0:
        logger.info("Updates were made, committing...")
        db.commit()
    else:
        logger.info("No updates were necessary")


def execute_migration(db: Session) -> None:
    select = text(
        """
SELECT e.id FROM externalintegrations AS e
  WHERE e.protocol = 'Overdrive'
    """
    )
    integrations = db.execute(select).fetchall()
    for integration_row in integrations:
        update_configuration(db, integration_row[0])


def main() -> None:
    session = production_session()
    try:
        execute_migration(session)
    finally:
        session.close()


main()
