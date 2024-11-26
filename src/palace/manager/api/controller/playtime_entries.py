from __future__ import annotations

import hashlib

import flask
from pydantic import ValidationError
from sqlalchemy import or_, select

from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.model.time_tracking import (
    PlaytimeEntriesPost,
    PlaytimeEntriesPostResponse,
)
from palace.manager.api.problem_details import NOT_FOUND_ON_REMOTE
from palace.manager.api.util.flask import get_request_library, get_request_patron
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.core.query.playtime_entries import PlaytimeEntries
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.util import get_one

MISSING_LOAN_IDENTIFIER = "LOAN_NOT_FOUND"


def resolve_loan_identifier(loan: Loan | None) -> str:
    def sha1(msg):
        return

    return (
        hashlib.sha1(f"loan: {loan.id}".encode()).hexdigest()
        if loan
        else MISSING_LOAN_IDENTIFIER
    )


class PlaytimeEntriesController(CirculationManagerController):
    def track_playtimes(self, collection_id, identifier_type, identifier_idn):
        library = get_request_library()
        identifier = get_one(
            self._db, Identifier, type=identifier_type, identifier=identifier_idn
        )
        collection = Collection.by_id(self._db, collection_id)

        if not identifier:
            return NOT_FOUND_ON_REMOTE.detailed(
                f"The identifier {identifier_type}/{identifier_idn} was not found."
            )
        if not collection:
            return NOT_FOUND_ON_REMOTE.detailed(
                f"The collection {collection_id} was not found."
            )

        if collection not in library.associated_collections:
            return INVALID_INPUT.detailed("Collection was not found in the Library.")

        if not identifier.licensed_through_collection(collection):
            return INVALID_INPUT.detailed(
                "This Identifier was not found in the Collection."
            )

        try:
            data = PlaytimeEntriesPost(**flask.request.json)
        except ValidationError as ex:
            return INVALID_INPUT.detailed(ex.json())

        # attempt to resolve a loan associated with the patron, identifier, in the time period
        entry_max_start_time = max([x.during_minute for x in data.time_entries])
        entry_min_end_time = min([x.during_minute for x in data.time_entries])

        loan = self._db.scalars(
            select(Loan)
            .select_from(Loan)
            .join(LicensePool)
            .where(
                LicensePool.identifier == identifier,
                Loan.patron == get_request_patron(),
                Loan.start <= entry_max_start_time,
                or_(Loan.end > entry_min_end_time, Loan.end == None),
            )
            .order_by(Loan.start.desc())
        ).first()

        loan_identifier = resolve_loan_identifier(loan=loan)

        responses, summary = PlaytimeEntries.insert_playtime_entries(
            self._db,
            identifier,
            collection,
            library,
            data,
            loan_identifier,
        )

        response_data = PlaytimeEntriesPostResponse(
            summary=summary, responses=responses
        )
        response = flask.jsonify(response_data.model_dump())
        response.status_code = 207
        return response
