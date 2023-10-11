from __future__ import annotations

import flask
from flask import url_for

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.controller import CirculationManagerController
from core.app_server import load_pagination_from_request
from core.classifier import genres
from core.feed.admin import AdminFeed
from core.feed.annotator.admin import AdminAnnotator
from core.util.problem_detail import ProblemDetail


class FeedController(CirculationManagerController, AdminPermissionsControllerMixin):
    def suppressed(self):
        self.require_librarian(flask.request.library)

        this_url = url_for("suppressed", _external=True)
        annotator = AdminAnnotator(self.circulation, flask.request.library)
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        opds_feed = AdminFeed.suppressed(
            _db=self._db,
            title="Hidden Books",
            url=this_url,
            annotator=annotator,
            pagination=pagination,
        )
        return opds_feed.as_response(max_age=0)

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
