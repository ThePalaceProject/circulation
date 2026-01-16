from __future__ import annotations

import flask
from flask import Response, url_for

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.controller.util import required_library_from_request
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.core.app_server import load_pagination_from_request
from palace.manager.core.classifier import genres
from palace.manager.core.opensearch import OpenSearchDocument
from palace.manager.feed.admin.suppressed import AdminSuppressedFeed, SuppressedFacets
from palace.manager.feed.annotator.admin.suppressed import AdminSuppressedAnnotator
from palace.manager.sqlalchemy.model.lane import Pagination
from palace.manager.util.problem_detail import ProblemDetail


class FeedController(CirculationManagerController, AdminPermissionsControllerMixin):
    def suppressed(self):
        library = required_library_from_request(flask.request)
        self.require_librarian(library)

        annotator = AdminSuppressedAnnotator(self.circulation, library)
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination

        # Load visibility facets from request
        facets = SuppressedFacets.from_request(flask.request.args.get)

        opds_feed = AdminSuppressedFeed.suppressed(
            _db=self._db,
            title="Hidden Books",
            annotator=annotator,
            pagination=pagination,
            facets=facets,
        )
        return opds_feed.as_response(max_age=0)

    def suppressed_search(self) -> Response | ProblemDetail:
        """Search within suppressed/hidden works for a library."""
        library = required_library_from_request(flask.request)
        self.require_librarian(library)

        pagination = load_pagination_from_request(
            Pagination, default_size=Pagination.DEFAULT_SEARCH_SIZE
        )
        if isinstance(pagination, ProblemDetail):
            return pagination

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        annotator = AdminSuppressedAnnotator(self.circulation, library)
        query = flask.request.args.get("q")

        # Generate URL for search endpoint
        search_url = url_for(
            "suppressed_search",
            library_short_name=library.short_name,
            _external=True,
        )

        if not query:
            # Return OpenSearch description document
            open_search_doc = self._suppressed_opensearch_document(search_url)
            headers = {"Content-Type": "application/opensearchdescription+xml"}
            return Response(open_search_doc, 200, headers)

        # Perform search within suppressed works
        result = AdminSuppressedFeed.suppressed_search(
            _db=self._db,
            title="Search Hidden Books",
            url=search_url,
            annotator=annotator,
            search_engine=search_engine,
            query=query,
            pagination=pagination,
        )
        if isinstance(result, ProblemDetail):
            return result
        return result.as_response(max_age=0)

    def _suppressed_opensearch_document(self, search_url: str) -> str:
        """Generate an OpenSearch description document for suppressed search."""
        info = {
            "name": "Search Hidden Books",
            "description": "Search within hidden/suppressed books",
            "tags": "hidden suppressed",
            "url_template": OpenSearchDocument.url_template(search_url),
        }
        info = OpenSearchDocument.escape_entities(info)
        return OpenSearchDocument.TEMPLATE % info

    def genres(self):
        data = dict({"Fiction": dict({}), "Nonfiction": dict({})})
        for name in genres:
            top = "Fiction" if genres[name].is_fiction else "Nonfiction"
            data[top][name] = dict(
                {
                    "name": name,
                    "parents": [parent.name for parent in genres[name].parents],
                    "subgenres": [subgenre.name for subgenre in genres[name].subgenres],
                }
            )
        return data
