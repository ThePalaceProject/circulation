from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.util.exceptions import PalaceValueError
from palace.util.log import LogLevel

from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.scripts.overdrive import OverdriveReaperScript
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestOverdriveReaperScript:

    def test_reap_all(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(LogLevel.info)
        with patch("palace.manager.scripts.overdrive.reap_all_collections") as mock:
            OverdriveReaperScript(db.session, services_fixture.services).do_run(
                ["--reap-all"]
            )
            mock.delay.assert_called_once_with()
            assert '"reap_all_collections" task has been queued' in caplog.text

    def test_reap_collection(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OverdriveAPI)
        with patch("palace.manager.scripts.overdrive.reap_collection") as mock:
            OverdriveReaperScript(db.session, services_fixture.services).do_run(
                ["--collection-name", collection.name]
            )
            mock.delay.assert_called_once_with(collection.id)
            assert '"reap_collection" task has been queued' in caplog.text

    def test_reap_collection_wrong_protocol(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        collection = db.collection()  # defaults to a non-Overdrive protocol
        with pytest.raises(PalaceValueError, match="not an Overdrive collection"):
            OverdriveReaperScript(db.session, services_fixture.services).do_run(
                ["--collection-name", collection.name]
            )

    def test_reap_collection_not_found(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        with pytest.raises(PalaceValueError, match="No collection found"):
            OverdriveReaperScript(db.session, services_fixture.services).do_run(
                ["--collection-name", "does-not-exist"]
            )

    def test_no_args_exits(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        with pytest.raises(SystemExit):
            OverdriveReaperScript(db.session, services_fixture.services).do_run([])
