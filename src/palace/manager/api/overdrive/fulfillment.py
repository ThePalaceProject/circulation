from __future__ import annotations

import flask

from palace.manager.circulation.fulfillment import RedirectFulfillment


class OverdriveManifestFulfillment(RedirectFulfillment):
    def __init__(self, content_link: str, scope_string: str, access_token: str) -> None:
        super().__init__(content_link)
        self.scope_string = scope_string
        self.access_token = access_token

    def response(self) -> flask.Response:
        headers = {
            "Location": self.content_link,
            "X-Overdrive-Scope": self.scope_string,
            "X-Overdrive-Patron-Authorization": f"Bearer {self.access_token}",
            "Content-Type": "text/plain",
        }
        return flask.Response("", 302, headers)
