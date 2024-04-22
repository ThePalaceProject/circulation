from __future__ import annotations

import flask


class StaticFileController:
    @staticmethod
    def static_file(directory, filename):
        return flask.send_from_directory(directory, filename)
