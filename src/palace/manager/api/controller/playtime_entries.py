from __future__ import annotations

import hashlib
from typing import Any

import flask
from pydantic import ValidationError
from sqlalchemy import or_, select

from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.model.time_tracking import (
    PlaytimeEntriesPost,
    PlaytimeEntriesPostResponse,
    PlaytimeEntriesPostSummary,
)
from palace.manager.api.util.flask import get_request_library, get_request_patron
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.core.query.playtime_entries import PlaytimeEntries
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Loan

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
        library = get_request_library(default=None)
        identifier, _ = Identifier.for_foreign_id(
            self._db, identifier_type, identifier_idn, autocreate=False
        )
        collection = Collection.by_id(self._db, collection_id)

        try:
            data = PlaytimeEntriesPost(**flask.request.json)
        except ValidationError as ex:
            return INVALID_INPUT.detailed(ex.json())

        # TODO: For the time being, we need to return a 207 multi-response instead
        #  of a 404 or 400 problem detail for missing or incorrectly associated
        #  libraries, collections, and identifiers. We can switch back to the problem
        #  detail responses once most instances of the client apps support them.
        #  We can remove the `_handle_unrecoverable_entries` function then, as well.
        if not library:
            return _handle_unrecoverable_entries(
                data,
                "The library was not found.",
            )

        if not identifier:
            return _handle_unrecoverable_entries(
                data,
                f"The identifier {identifier_type}/{identifier_idn} was not found.",
            )
        if not collection:
            return _handle_unrecoverable_entries(
                data, f"The collection {collection_id} was not found."
            )

        if collection not in library.associated_collections:
            return _handle_unrecoverable_entries(
                data, "Collection was not found in the Library."
            )

        if not identifier.licensed_through_collection(collection):
            return _handle_unrecoverable_entries(
                data, "This Identifier was not found in the Collection."
            )

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

        return make_response(responses, summary)


# TODO: We can remove this function once we switch back to problem
#  detail responses (see comment above).
def _handle_unrecoverable_entries(
    data: PlaytimeEntriesPost, reason: str, status: int = 410
):
    entries = data.time_entries
    count = len(entries)
    summary = PlaytimeEntriesPostSummary(failures=count, total=count)
    entry_responses = [
        dict(id=entry.id, status=status, message=reason) for entry in entries
    ]
    return make_response(entry_responses, summary)


def make_response(
    response_entries: list[dict[str, Any]], summary: PlaytimeEntriesPostSummary
):
    response_data = PlaytimeEntriesPostResponse(
        summary=summary, responses=response_entries
    )
    response = flask.jsonify(response_data.model_dump())
    response.status_code = 207
    return response
