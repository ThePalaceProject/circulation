from __future__ import annotations

import flask
from pydantic import ValidationError

from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.model.time_tracking import (
    PlaytimeEntriesPost,
    PlaytimeEntriesPostResponse,
)
from palace.manager.api.problem_details import NOT_FOUND_ON_REMOTE
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.core.query.playtime_entries import PlaytimeEntries
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one


class PlaytimeEntriesController(CirculationManagerController):
    def track_playtimes(self, collection_id, identifier_type, identifier_idn):
        library: Library = flask.request.library
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

        if collection not in library.collections:
            return INVALID_INPUT.detailed("Collection was not found in the Library.")

        if not identifier.licensed_through_collection(collection):
            return INVALID_INPUT.detailed(
                "This Identifier was not found in the Collection."
            )

        try:
            data = PlaytimeEntriesPost(**flask.request.json)
        except ValidationError as ex:
            return INVALID_INPUT.detailed(ex.json())

        responses, summary = PlaytimeEntries.insert_playtime_entries(
            self._db, identifier, collection, library, data
        )

        response_data = PlaytimeEntriesPostResponse(
            summary=summary, responses=responses
        )
        response = flask.jsonify(response_data.dict())
        response.status_code = 207
        return response
