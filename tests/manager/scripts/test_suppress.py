from __future__ import annotations

import textwrap
from datetime import datetime, timezone
from unittest.mock import create_autospec, patch

import pytest

from palace.manager.scripts.suppress import SuppressResult, SuppressWorkForLibraryScript
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

        assert "error:" in capsys.readouterr().err

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
        assert parsed.dry_run is False

    def test_parse_command_line_with_file(self, db: DatabaseTransactionFixture):
        parsed = SuppressWorkForLibraryScript.parse_command_line(
            db.session,
            [
                "--library",
                "lib1",
                "--identifier-type",
                "ISBN",
                "--file",
                "/tmp/ids.csv",
            ],
        )
        assert parsed.library == "lib1"
        assert parsed.file == "/tmp/ids.csv"
        assert parsed.identifier is None

    def test_parse_command_line_dry_run(self, db: DatabaseTransactionFixture):
        parsed = SuppressWorkForLibraryScript.parse_command_line(
            db.session,
            ["--library", "lib1", "--identifier", "123", "--dry-run"],
        )
        assert parsed.dry_run is True

    def test_parse_command_line_file_and_identifier_mutually_exclusive(
        self, db: DatabaseTransactionFixture, capsys
    ):
        with pytest.raises(SystemExit):
            SuppressWorkForLibraryScript.parse_command_line(
                db.session,
                [
                    "--library",
                    "lib1",
                    "--identifier",
                    "123",
                    "--file",
                    "/tmp/ids.csv",
                ],
            )
        assert "error:" in capsys.readouterr().err

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

    def test_load_identifiers_from_file(self, db: DatabaseTransactionFixture, tmp_path):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            textwrap.dedent(
                """\
                identifier,identifier_type
                978-0-06-112008-4,ISBN
                12345,Overdrive ID
                ,ISBN
            """
            )
        )

        script = SuppressWorkForLibraryScript(db.session)
        pairs = script.load_identifiers_from_file(str(csv_file), "ISBN")

        assert pairs == [
            ("ISBN", "978-0-06-112008-4"),
            ("Overdrive ID", "12345"),
        ]

    def test_load_identifiers_from_file_no_type_column(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            textwrap.dedent(
                """\
                identifier
                978-0-06-112008-4
                12345
            """
            )
        )

        script = SuppressWorkForLibraryScript(db.session)
        pairs = script.load_identifiers_from_file(str(csv_file), "ISBN")

        assert pairs == [
            ("ISBN", "978-0-06-112008-4"),
            ("ISBN", "12345"),
        ]

    def test_load_identifiers_from_file_empty_type_falls_back_to_default(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            textwrap.dedent(
                """\
                identifier,identifier_type
                978-0-06-112008-4,
                12345,Overdrive ID
            """
            )
        )

        script = SuppressWorkForLibraryScript(db.session)
        pairs = script.load_identifiers_from_file(str(csv_file), "ISBN")

        assert pairs == [
            ("ISBN", "978-0-06-112008-4"),
            ("Overdrive ID", "12345"),
        ]

    def test_load_identifiers_from_file_with_duplicates(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            textwrap.dedent(
                """\
                identifier,identifier_type
                978-0-06-112008-4,ISBN
                978-0-06-112008-4,ISBN
            """
            )
        )

        script = SuppressWorkForLibraryScript(db.session)
        pairs = script.load_identifiers_from_file(str(csv_file), "ISBN")

        assert pairs == [
            ("ISBN", "978-0-06-112008-4"),
            ("ISBN", "978-0-06-112008-4"),
        ]

    def test_load_identifiers_from_file_missing_identifier_column(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text("foo,bar\n1,2\n")

        script = SuppressWorkForLibraryScript(db.session)
        with pytest.raises(ValueError, match='must contain an "identifier" column'):
            script.load_identifiers_from_file(str(csv_file), "ISBN")

    def test_do_run(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="test")
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        suppress_work_mock = create_autospec(script.suppress_work)
        suppress_work_mock.return_value = SuppressResult.NEWLY_SUPPRESSED
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

        suppress_work_mock.assert_called_once_with(
            test_library, test_identifier, dry_run=False
        )

    def test_do_run_dry_run(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="test")
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        suppress_work_mock = create_autospec(script.suppress_work)
        suppress_work_mock.return_value = SuppressResult.NEWLY_SUPPRESSED
        script.suppress_work = suppress_work_mock
        args = [
            "--library",
            test_library.short_name,
            "--identifier-type",
            test_identifier.type,
            "--identifier",
            test_identifier.identifier,
            "--dry-run",
        ]
        script.do_run(args)

        suppress_work_mock.assert_called_once_with(
            test_library, test_identifier, dry_run=True
        )

    def test_do_run_with_file(self, db: DatabaseTransactionFixture, tmp_path, capsys):
        test_library = db.library(short_name="test")
        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            f"identifier,identifier_type\n"
            f"{id1.identifier},{id1.type}\n"
            f"{id2.identifier},{id2.type}\n"
        )

        script = SuppressWorkForLibraryScript(db.session)
        script.do_run(
            [
                "--library",
                test_library.short_name,
                "--file",
                str(csv_file),
            ]
        )

        assert test_library in work1.suppressed_for
        assert test_library in work2.suppressed_for

        out = capsys.readouterr().out
        assert "Newly suppressed:    2" in out
        assert "Already suppressed:  0" in out
        assert "Not found:           0" in out

    def test_suppress_work(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)

        assert work.suppressed_for == []

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, work.presentation_edition.primary_identifier
        )

        assert result == SuppressResult.NEWLY_SUPPRESSED
        assert work.suppressed_for == [test_library]

    def test_suppress_work_already_suppressed(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)
        work.suppressed_for.append(test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, work.presentation_edition.primary_identifier
        )

        assert result == SuppressResult.ALREADY_SUPPRESSED
        assert work.suppressed_for == [test_library]

    def test_suppress_work_no_work_for_identifier(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, identifier)

        assert result == SuppressResult.NOT_FOUND

    def test_suppress_work_dry_run(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library,
            work.presentation_edition.primary_identifier,
            dry_run=True,
        )

        assert result == SuppressResult.NEWLY_SUPPRESSED
        assert work.suppressed_for == []

    def test_suppress_work_dry_run_already_suppressed(
        self, db: DatabaseTransactionFixture
    ):
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)
        work.suppressed_for.append(test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library,
            work.presentation_edition.primary_identifier,
            dry_run=True,
        )

        assert result == SuppressResult.ALREADY_SUPPRESSED

    def test_print_results_normal(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="mylib", name="My Library")
        script = SuppressWorkForLibraryScript(db.session)
        results = {
            ("ISBN", "111"): SuppressResult.NEWLY_SUPPRESSED,
            ("ISBN", "222"): SuppressResult.ALREADY_SUPPRESSED,
            ("ISBN", "333"): SuppressResult.NOT_FOUND,
        }
        started_at = datetime(2026, 2, 26, 12, 0, 0, tzinfo=timezone.utc)
        script._print_results(
            results,
            dry_run=False,
            library=test_library,
            started_at=started_at,
            duration_seconds=1.23,
        )

        out = capsys.readouterr().out
        assert "Suppression Results Summary" in out
        assert "My Library (mylib)" in out
        assert "2026-02-26 12:00:00 UTC" in out
        assert "1.23s" in out
        assert "Newly suppressed:    1" in out
        assert "Already suppressed:  1" in out
        assert "Not found:           1" in out
        assert "[SUPPRESSED] ISBN/111" in out
        assert "[ALREADY SUPPRESSED] ISBN/222" in out
        assert "[NOT FOUND] ISBN/333" in out
        assert "[DRY RUN]" not in out

    def test_print_results_dry_run(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="mylib", name="My Library")
        script = SuppressWorkForLibraryScript(db.session)
        results = {
            ("ISBN", "111"): SuppressResult.NEWLY_SUPPRESSED,
            ("ISBN", "222"): SuppressResult.NOT_FOUND,
        }
        started_at = datetime(2026, 2, 26, 9, 30, 0, tzinfo=timezone.utc)
        script._print_results(
            results,
            dry_run=True,
            library=test_library,
            started_at=started_at,
            duration_seconds=0.05,
        )

        out = capsys.readouterr().out
        assert "[DRY RUN] Suppression Results Summary" in out
        assert "My Library (mylib)" in out
        assert "2026-02-26 09:30:00 UTC" in out
        assert "0.05s" in out
        assert "Would suppress:      1" in out
        assert "Not found:           1" in out
        assert "[WOULD SUPPRESS] ISBN/111" in out
        assert "[NOT FOUND] ISBN/222" in out

    def test_do_run_not_found_identifier(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="test")

        script = SuppressWorkForLibraryScript(db.session)
        script.do_run(
            [
                "--library",
                test_library.short_name,
                "--identifier-type",
                "ISBN",
                "--identifier",
                "nonexistent-id",
            ]
        )

        out = capsys.readouterr().out
        assert "Newly suppressed:    0" in out
        assert "Not found:           1" in out
        assert "[NOT FOUND] ISBN/nonexistent-id" in out

    def test_do_run_commits_once_for_all_suppressions(
        self, db: DatabaseTransactionFixture, tmp_path, capsys
    ):
        test_library = db.library(short_name="test")
        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            f"identifier,identifier_type\n"
            f"{id1.identifier},{id1.type}\n"
            f"{id2.identifier},{id2.type}\n"
        )

        script = SuppressWorkForLibraryScript(db.session)
        with patch.object(db.session, "commit", wraps=db.session.commit) as mock_commit:
            script.do_run(
                ["--library", test_library.short_name, "--file", str(csv_file)]
            )
            mock_commit.assert_called_once()

        assert test_library in work1.suppressed_for
        assert test_library in work2.suppressed_for

    def test_do_run_rolls_back_all_on_commit_failure(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        test_library = db.library(short_name="test")
        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            f"identifier,identifier_type\n"
            f"{id1.identifier},{id1.type}\n"
            f"{id2.identifier},{id2.type}\n"
        )

        script = SuppressWorkForLibraryScript(db.session)
        with patch.object(
            db.session, "commit", side_effect=Exception("DB error")
        ), patch.object(db.session, "rollback") as mock_rollback:
            with pytest.raises(Exception, match="DB error"):
                script.do_run(
                    ["--library", test_library.short_name, "--file", str(csv_file)]
                )
            mock_rollback.assert_called_once()

    def test_do_run_rolls_back_on_unexpected_error_during_processing(
        self, db: DatabaseTransactionFixture
    ):
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier

        script = SuppressWorkForLibraryScript(db.session)
        with patch.object(
            script, "suppress_work", side_effect=RuntimeError("unexpected")
        ), patch.object(db.session, "rollback") as mock_rollback:
            with pytest.raises(RuntimeError, match="unexpected"):
                script.do_run(
                    [
                        "--library",
                        test_library.short_name,
                        "--identifier-type",
                        identifier.type,
                        "--identifier",
                        identifier.identifier,
                    ]
                )
            mock_rollback.assert_called_once()

    def test_do_run_dry_run_does_not_commit(
        self, db: DatabaseTransactionFixture, capsys
    ):
        test_library = db.library(short_name="test")
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        with patch.object(db.session, "commit") as mock_commit:
            script.do_run(
                [
                    "--library",
                    test_library.short_name,
                    "--identifier-type",
                    test_identifier.type,
                    "--identifier",
                    test_identifier.identifier,
                    "--dry-run",
                ]
            )
        mock_commit.assert_not_called()

    def test_load_identifiers_from_file_not_found(self, db: DatabaseTransactionFixture):
        script = SuppressWorkForLibraryScript(db.session)
        with pytest.raises(ValueError, match="CSV file not found"):
            script.load_identifiers_from_file("/nonexistent/path/ids.csv", "ISBN")

    def test_suppress_work_does_not_commit(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)

        script = SuppressWorkForLibraryScript(db.session)
        with patch.object(db.session, "commit") as mock_commit:
            result = script.suppress_work(
                test_library, work.presentation_edition.primary_identifier
            )
        assert result == SuppressResult.NEWLY_SUPPRESSED
        mock_commit.assert_not_called()
