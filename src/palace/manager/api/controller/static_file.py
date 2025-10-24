from __future__ import annotations

from pathlib import Path

import flask


class StaticFileController:
    @staticmethod
    def static_file(directory: str | Path, filename: str) -> flask.Response:
        return flask.send_from_directory(directory, filename)
