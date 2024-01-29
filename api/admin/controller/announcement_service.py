from __future__ import annotations

from collections.abc import Callable
from typing import Any

import flask
from sqlalchemy.orm import Session

from api.admin.announcement_list_validator import AnnouncementListValidator
from api.config import Configuration
from core.model.announcements import Announcement
from core.problem_details import INVALID_INPUT
from core.util.problem_detail import ProblemDetail, ProblemError


class AnnouncementSettings:
    """Controller that manages global announcements for all libraries"""

    def __init__(self, db: Session) -> None:
        self._db = db

    def _action(self) -> Callable:
        method = flask.request.method.lower()
        return getattr(self, method)

    def process_many(self) -> dict[str, Any] | ProblemDetail:
        try:
            return self._action()()
        except ProblemError as e:
            return e.problem_detail

    def get(self) -> dict[str, Any]:
        """Respond with settings and all global announcements"""
        db_announcements = (
            self._db.execute(Announcement.global_announcements()).scalars().all()
        )
        announcements = [x.to_data().as_dict() for x in db_announcements]
        settings = Configuration.ANNOUNCEMENT_SETTINGS
        return dict(
            settings=settings,
            announcements=announcements,
        )

    def post(self) -> dict[str, Any]:
        """POST multiple announcements to the global namespace"""
        validator = AnnouncementListValidator()
        if flask.request.form is None or "announcements" not in flask.request.form:
            raise ProblemError(problem_detail=INVALID_INPUT)
        validated_announcements = validator.validate_announcements(
            flask.request.form["announcements"]
        )

        # Sync the announcements in the database with the validated announcements
        existing_announcements = (
            self._db.execute(Announcement.global_announcements()).scalars().all()
        )
        Announcement.sync(self._db, existing_announcements, validated_announcements)

        return dict(success=True)
