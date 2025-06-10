from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.manager.scripts.saml import UpdateSamlMetadata
from palace.manager.service.logging.configuration import LogLevel
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestSamlUpdateScript:

    def test_saml_update_script(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)
        with patch(
            "palace.manager.scripts.saml.update_saml_federation_idps_metadata"
        ) as update_mock:
            UpdateSamlMetadata(db.session).run()
            assert update_mock.delay.call_count == 1
            assert (
                'The "update_saml_federation_idps_metadata" task has been queued for execution.'
                in caplog.text
            )
