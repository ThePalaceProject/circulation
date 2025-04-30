from unittest.mock import create_autospec, patch

import pytest

from palace.manager.api.metadata.novelist import NoveListAPI
from palace.manager.celery.tasks.novelist import (
    update_novelists_by_library,
    update_novelists_for_all_libraries,
)
from palace.manager.service.logging.configuration import LogLevel
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


def test_update_novelists_for_all_libraries(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)

    lib1 = db.library(name="lib1", short_name="lib1")
    lib2 = db.library(name="lib2", short_name="lib2")

    with patch(
        "palace.manager.celery.tasks.novelist.update_novelists_by_library"
    ) as update:
        update_novelists_for_all_libraries.delay().wait()

        assert update.delay.call_count == 2
        assert [x.kwargs["library_id"] for x in update.delay.call_args_list] == [
            lib1.id,
            lib2.id,
        ]

    for lib in [lib1, lib2]:
        assert (
            f"Queued update task for library('{lib.name}' (id={lib.id})" in caplog.text
        )

    assert "task completed successfully" in caplog.text


def test_update_novelists_by_library(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)

    lib1 = db.library(name="lib1", short_name="lib1")
    with patch("palace.manager.celery.tasks.novelist.NoveListAPI") as api_class:
        mock_api = create_autospec(NoveListAPI)
        response = "test response"
        mock_api.put_items_novelist.return_value = response
        api_class.from_config.return_value = mock_api
        update_novelists_by_library.delay(library_id=lib1.id).wait()

        api_class.from_config.assert_called_once_with(lib1)
        mock_api.put_items_novelist.assert_called_once_with(lib1)
        assert f"Novelist API Response:\n{response}" in caplog.text


def test_update_novelists_by_library_library_not_found(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)
    update_novelists_by_library.delay(library_id=100).wait()
    assert f"Library with id=100 not found. Unable to process task." in caplog.text
