from __future__ import annotations

from unittest.mock import create_autospec

import pytest

from palace.manager.scripts.suppress import SuppressWorkForLibraryScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestSuppressWorkForLibraryScript:
    @pytest.mark.parametrize(
        "cmd_args",
        [
            "",
            "--library test",
            "--library test  --identifier-type test",
            "--identifier-type test",
            "--identifier test",
        ],
    )
    def test_parse_command_line_error(
        self, db: DatabaseTransactionFixture, capsys, cmd_args: str
    ):
        with pytest.raises(SystemExit):
            SuppressWorkForLibraryScript.parse_command_line(
                db.session, cmd_args.split(" ")
            )

        assert "error: the following arguments are required" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "cmd_args",
        [
            "--library test1 --identifier-type test2 --identifier test3",
            "-l test1 -t test2 -i test3",
        ],
    )
    def test_parse_command_line(self, db: DatabaseTransactionFixture, cmd_args: str):
        parsed = SuppressWorkForLibraryScript.parse_command_line(
            db.session, cmd_args.split(" ")
        )
        assert parsed.library == "test1"
        assert parsed.identifier_type == "test2"
        assert parsed.identifier == "test3"

    def test_load_library(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")

        script = SuppressWorkForLibraryScript(db.session)
        loaded_library = script.load_library("test")
        assert loaded_library == test_library

        with pytest.raises(ValueError):
            script.load_library("test2")

    def test_load_identifier(self, db: DatabaseTransactionFixture):
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        loaded_identifier = script.load_identifier(
            str(test_identifier.type), str(test_identifier.identifier)
        )
        assert loaded_identifier == test_identifier

        loaded_identifier = script.load_identifier(
            script.BY_DATABASE_ID, str(test_identifier.id)
        )
        assert loaded_identifier == test_identifier

        with pytest.raises(ValueError):
            script.load_identifier("test", "test")

    def test_do_run(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        suppress_work_mock = create_autospec(script.suppress_work)
        script.suppress_work = suppress_work_mock
        args = [
            "--library",
            test_library.short_name,
            "--identifier-type",
            test_identifier.type,
            "--identifier",
            test_identifier.identifier,
        ]
        script.do_run(args)

        suppress_work_mock.assert_called_once_with(test_library, test_identifier)

    def test_suppress_work(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)

        assert work.suppressed_for == []

        script = SuppressWorkForLibraryScript(db.session)
        script.suppress_work(test_library, work.presentation_edition.primary_identifier)

        assert work.suppressed_for == [test_library]
