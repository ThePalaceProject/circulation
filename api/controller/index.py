from __future__ import annotations

import flask
from flask import Response, redirect, url_for

from api.controller.circulation_manager import CirculationManagerController
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from core.util.problem_detail import ProblemDetail


class IndexController(CirculationManagerController):
    """Redirect the patron to the appropriate feed."""

    def __call__(self):
        # If this library provides a custom index view, use that.
        library = flask.request.library
        custom = self.manager.custom_index_views.get(library.id)
        if custom is not None:
            annotator = self.manager.annotator(None)
            return custom(library, annotator)

        # The simple case: the app is equally open to all clients.
        library_short_name = flask.request.library.short_name
        if not self.has_root_lanes():
            return redirect(
                url_for(
                    "acquisition_groups",
                    library_short_name=library_short_name,
                    _external=True,
                )
            )

        # The more complex case. We must authorize the patron, check
        # their type, and redirect them to an appropriate feed.
        return self.appropriate_index_for_patron_type()

    def authentication_document(self):
        """Serve this library's Authentication For OPDS document."""
        return Response(
            self.manager.authentication_for_opds_document,
            200,
            {"Content-Type": AuthenticationForOPDSDocument.MEDIA_TYPE},
        )

    def has_root_lanes(self):
        """Does the active library feature root lanes for patrons of
        certain types?

        :return: A boolean
        """
        return flask.request.library.has_root_lanes

    def authenticated_patron_root_lane(self):
        patron = self.authenticated_patron_from_request()
        if isinstance(patron, ProblemDetail):
            return patron
        if isinstance(patron, Response):
            return patron
        return patron.root_lane

    def appropriate_index_for_patron_type(self):
        library_short_name = flask.request.library.short_name
        root_lane = self.authenticated_patron_root_lane()
        if isinstance(root_lane, ProblemDetail):
            return root_lane
        if isinstance(root_lane, Response):
            return root_lane
        if root_lane is None:
            return redirect(
                url_for(
                    "acquisition_groups",
                    library_short_name=library_short_name,
                    _external=True,
                )
            )

        return redirect(
            url_for(
                "acquisition_groups",
                library_short_name=library_short_name,
                lane_identifier=root_lane.id,
                _external=True,
            )
        )
