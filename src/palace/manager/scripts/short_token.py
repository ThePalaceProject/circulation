from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import TextIO

from sqlalchemy.orm import Session

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.authenticator import LibraryAuthenticator
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail


class GenerateShortTokenScript(LibraryInputScript):
    """
    Generate a short client token of the specified duration that can be used for testing that
    involves the Adobe Vendor ID API implementation.
    """

    @classmethod
    def arg_parser(
        cls, _db: Session, multiple_libraries: bool = False
    ) -> argparse.ArgumentParser:
        parser = super().arg_parser(_db, multiple_libraries=multiple_libraries)
        parser.add_argument(
            "--barcode",
            help="The patron barcode.",
            required=True,
        )
        parser.add_argument("--pin", help="The patron pin.")
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--days",
            help="Token expiry in days.",
            type=int,
        )
        group.add_argument(
            "--hours",
            help="Token expiry in hours.",
            type=int,
        )
        group.add_argument(
            "--minutes",
            help="Token expiry in minutes.",
            type=int,
        )
        return parser

    def do_run(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        output: TextIO = sys.stdout,
        authdata: AuthdataUtility | None = None,
    ) -> None:
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)

        if len(args.libraries) != 1:
            output.write("Library not found!\n")
            sys.exit(-1)
        library = args.libraries[0]

        # First try to shortcut full authentication, by just looking up patron directly
        patron: Patron | ProblemDetail | None = get_one(
            _db, Patron, authorization_identifier=args.barcode
        )
        if patron is None:
            # Fall back to a full patron lookup
            auth = LibraryAuthenticator.from_config(
                _db, args.libraries[0]
            ).basic_auth_provider
            if auth is None:
                output.write("No methods to authenticate patron found!\n")
                sys.exit(-1)
            patron = auth.authenticate(
                _db, credentials={"username": args.barcode, "password": args.pin}
            )
        if not isinstance(patron, Patron):
            output.write(f"Patron not found {args.barcode}!\n")
            sys.exit(-1)

        authdata_util = authdata
        if authdata_util is None:
            authdata_util = AuthdataUtility.from_config(library, _db)
        if authdata_util is None:
            output.write(
                "Library not registered with library registry! Please register and try again."
            )
            sys.exit(-1)

        patron_identifier = authdata_util._adobe_patron_identifier(patron)
        expires = {
            k: v
            for (k, v) in vars(args).items()
            if k in ["days", "hours", "minutes"] and v is not None
        }
        vendor_id, token = authdata_util.encode_short_client_token(
            patron_identifier, expires=expires
        )
        username, password = token.rsplit("|", 1)

        output.write(f"Vendor ID: {vendor_id}\n")
        output.write(f"Token: {token}\n")
        output.write(f"Username: {username}\n")
        output.write(f"Password: {password}\n")
