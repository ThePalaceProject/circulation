from __future__ import annotations

import argparse
import time
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.scripts.input import PatronInputScript
from palace.manager.sqlalchemy.model.patron import Patron


class AdobeAccountIDResetScript(PatronInputScript):
    @classmethod
    def arg_parser(
        cls, _db: Session, multiple_libraries: bool = False
    ) -> argparse.ArgumentParser:
        parser = super().arg_parser(_db, multiple_libraries=multiple_libraries)
        parser.add_argument(
            "--delete",
            help="Actually delete credentials as opposed to showing what would happen.",
            action="store_true",
        )
        return parser

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        patrons = parsed.patrons
        self.delete = parsed.delete
        if not self.delete:
            self.log.info(
                "This is a dry run. Nothing will actually change in the database."
            )
            self.log.info("Run with --delete to change the database.")

        if patrons and self.delete:
            self.log.warning(
                """This is not a drill.
Running this script will permanently disconnect %d patron(s) from their Adobe account IDs.
They will be unable to fulfill any existing loans that involve Adobe-encrypted files.
Sleeping for five seconds to give you a chance to back out.
You'll get another chance to back out before the database session is committed.""",
                len(patrons),
            )
            time.sleep(5)
        self.process_patrons(patrons)
        if self.delete:
            self.log.warning("All done. Sleeping for five seconds before committing.")
            time.sleep(5)
            self._db.commit()

    def process_patron(self, patron: Patron) -> None:
        """Delete all of a patron's Credentials that contain an Adobe account
        ID _or_ connect the patron to a DelegatedPatronIdentifier that
        contains an Adobe account ID.
        """
        self.log.info(
            'Processing patron "%s"',
            patron.authorization_identifier
            or patron.username
            or patron.external_identifier,
        )
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self.log.info(
                ' Deleting "%s" credential "%s"', credential.type, credential.credential
            )
            if self.delete:
                self._db.delete(credential)
