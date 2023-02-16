from __future__ import annotations

import json
from typing import TYPE_CHECKING, Callable

import flask

from api.announcements import Announcements
from api.config import Configuration
from core.model.configuration import ConfigurationSetting
from core.problem_details import INVALID_INPUT

from . import SettingsController

if TYPE_CHECKING:
    from core.util.problem_detail import ProblemDetail


class AnnouncementSettings(SettingsController):
    """Controller that manages global announcements for all libraries"""

    def _action(self) -> Callable:
        method = flask.request.method.lower()
        return getattr(self, method)

    def process_many(self) -> dict | ProblemDetail:
        return self._action()()

    def process_one(self, key) -> dict | ProblemDetail:
        return self._action()(key=key)

    def get(self) -> dict | ProblemDetail:
        """Respond with settings and all global announcements"""
        announcements = Announcements.for_all(self._db)
        settings = Configuration.ANNOUNCEMENT_SETTINGS
        return dict(
            settings=settings,
            announcements=[ann.json_ready for ann in announcements.announcements],
        )

    def post(self) -> dict | ProblemDetail:
        """POST multiple announcements to the global namespace, all announcements are overwritten"""
        try:
            announcements = Announcements(flask.request.form["announcements"])
            if announcements.problem:
                return announcements.problem
        except (KeyError, TypeError):
            return INVALID_INPUT

        conf = ConfigurationSetting.sitewide(
            self._db, Announcements.GLOBAL_SETTING_NAME
        )
        conf.value = json.dumps([ann.json_ready for ann in announcements.announcements])

        return dict(success=True)
