from __future__ import annotations

from palace.manager.api.overdrive.fulfillment import OverdriveManifestFulfillment
from tests.fixtures.overdrive import OverdriveAPIFixture


class TestOverdriveManifestFulfillment:
    def test_response(self, overdrive_api_fixture: OverdriveAPIFixture) -> None:
        db = overdrive_api_fixture.db

        # An OverdriveManifestFulfillment just redirects the client
        # directly to the manifest file
        info = OverdriveManifestFulfillment(
            "http://content-link/",
            "scope string",
            "access token",
        )
        response = info.response()
        assert 302 == response.status_code
        assert "" == response.get_data(as_text=True)
        headers = response.headers
        assert "text/plain" == headers["Content-Type"]

        # These are the important headers; the location of the manifest file
        # and the scope necessary to initiate Patron Authentication for
        # it.
        assert "scope string" == headers["X-Overdrive-Scope"]
        assert "Bearer access token" == headers["X-Overdrive-Patron-Authorization"]
        assert "http://content-link/" == headers["Location"]
