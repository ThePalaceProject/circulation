from __future__ import annotations

import os

import flask

from api.config import Configuration
from api.controller.circulation_manager import CirculationManagerController
from core.model import ConfigurationSetting


class StaticFileController(CirculationManagerController):
    def static_file(self, directory, filename):
        max_age = ConfigurationSetting.sitewide(
            self._db, Configuration.STATIC_FILE_CACHE_TIME
        ).int_value
        return flask.send_from_directory(directory, filename, max_age=max_age)

    def image(self, filename):
        directory = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "..",
            "..",
            "resources",
            "images",
        )
        return self.static_file(directory, filename)
