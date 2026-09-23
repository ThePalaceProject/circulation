from __future__ import annotations

import re
import textwrap
from datetime import datetime, timezone
from unittest.mock import create_autospec, patch

import pytest

from palace.manager.scripts.suppress import (
    SuppressOutcome,
    SuppressResult,
    SuppressWorkForLibraryScript,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
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

    def test_parse_command_line_suppress_ambiguous_default_false(
        self, db: DatabaseTransactionFixture
    ):
        parsed = SuppressWorkForLibraryScript.parse_command_line(
            db.session,
            ["--library", "lib1", "--identifier", "123"],
        )
        assert parsed.suppress_ambiguous is False

    def test_parse_command_line_suppress_ambiguous_flag(
        self, db: DatabaseTransactionFixture
    ):
        parsed = SuppressWorkForLibraryScript.parse_command_line(
            db.session,
            ["--library", "lib1", "--identifier", "123", "--suppress-ambiguous"],
        )
        assert parsed.suppress_ambiguous is True

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

    def test_load_identifiers_from_file_omitted_type_value_falls_back_to_default(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        """When identifier_type column exists but a row omits the value (e.g. '12345'
        instead of '12345,'), DictReader sets it to None. We must not call .strip()
        on None."""
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            textwrap.dedent(
                """\
                identifier,identifier_type
                978-0-06-112008-4
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

    def test_do_run_deduplicates_and_warns(
        self, db: DatabaseTransactionFixture, tmp_path, caplog
    ):
        """When CSV contains duplicate identifiers, they are deduplicated before
        processing and a warning is logged."""
        import logging

        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            f"identifier,identifier_type\n"
            f"{id1.identifier},{id1.type}\n"
            f"{id1.identifier},{id1.type}\n"
            f"{id2.identifier},{id2.type}\n"
        )

        caplog.set_level(logging.WARNING)
        script = SuppressWorkForLibraryScript(db.session)
        script.do_run(["--library", test_library.short_name, "--file", str(csv_file)])

        assert "Removed 1 duplicate identifier(s) from input" in caplog.text
        assert test_library in work1.suppressed_for
        assert test_library in work2.suppressed_for

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
        suppress_work_mock.return_value = SuppressOutcome(
            SuppressResult.NEWLY_SUPPRESSED, "Some Title"
        )
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
            test_library, test_identifier, dry_run=False, suppress_ambiguous=False
        )

    def test_do_run_dry_run(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="test")
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        suppress_work_mock = create_autospec(script.suppress_work)
        suppress_work_mock.return_value = SuppressOutcome(
            SuppressResult.NEWLY_SUPPRESSED, "Some Title"
        )
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
            test_library, test_identifier, dry_run=True, suppress_ambiguous=False
        )

    def test_do_run_suppress_ambiguous_flag(
        self, db: DatabaseTransactionFixture, capsys
    ):
        test_library = db.library(short_name="test")
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        suppress_work_mock = create_autospec(script.suppress_work)
        suppress_work_mock.return_value = SuppressOutcome(
            SuppressResult.NEWLY_SUPPRESSED, "Some Title"
        )
        script.suppress_work = suppress_work_mock
        args = [
            "--library",
            test_library.short_name,
            "--identifier-type",
            test_identifier.type,
            "--identifier",
            test_identifier.identifier,
            "--suppress-ambiguous",
        ]
        script.do_run(args)

        suppress_work_mock.assert_called_once_with(
            test_library, test_identifier, dry_run=False, suppress_ambiguous=True
        )

    def test_do_run_with_file(self, db: DatabaseTransactionFixture, tmp_path, capsys):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
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
        assert re.search(r"Newly suppressed:\s+2", out)
        assert re.search(r"Already suppressed:\s+0", out)
        assert re.search(r"Not found:\s+0", out)

    def test_suppress_work(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)

        assert work.suppressed_for == []

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, work.presentation_edition.primary_identifier
        )

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == [test_library]

    def test_suppress_work_already_suppressed(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)
        work.suppressed_for.append(test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, work.presentation_edition.primary_identifier
        )

        assert result.result == SuppressResult.ALREADY_SUPPRESSED
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == [test_library]

    def test_suppress_work_no_work_for_identifier(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        db.collection(library=test_library)
        identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, identifier)

        assert result.result == SuppressResult.NOT_FOUND
        assert result.description is None

    def test_suppress_work_library_with_no_collections(
        self, db: DatabaseTransactionFixture
    ):
        """A library that carries no collections carries no works, so
        there is nothing for it to suppress -- but the work does exist,
        so this is reported as not-in-this-library rather than not-found."""
        test_library = db.library(short_name="test")
        work = db.work(with_license_pool=True)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, work.presentation_edition.primary_identifier
        )

        assert result.result == SuppressResult.NOT_IN_LIBRARY
        assert work.suppressed_for == []

    def test_library_collection_ids_are_cached(self, db: DatabaseTransactionFixture):
        """load_works consults this once per identifier, so a --file run
        would otherwise repeat the same query for every row."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)

        script = SuppressWorkForLibraryScript(db.session)
        first = script._library_collection_ids(test_library)
        second = script._library_collection_ids(test_library)

        assert first == [collection.id]
        assert first is second

    def test_suppress_work_pool_without_a_work(self, db: DatabaseTransactionFixture):
        """A LicensePool can exist before its Work has been calculated
        (work_id is nullable), so there may be nothing to suppress even
        though the identifier is licensed by the library."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        edition = db.edition()
        db.licensepool(edition, collection=collection)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, edition.primary_identifier)

        assert result.result == SuppressResult.NOT_FOUND

    def test_suppress_work_not_in_library_distinguished_from_not_found(
        self, db: DatabaseTransactionFixture
    ):
        """An identifier belonging to some other library's collection is a
        different problem from an identifier that matches nothing at all,
        so the two get distinct results instead of both reading NOT FOUND."""
        test_library = db.library(short_name="test")
        db.collection(library=test_library)
        other_collection = db.collection()
        work = db.work(with_license_pool=True, collection=other_collection)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, work.presentation_edition.primary_identifier
        )

        assert result.result == SuppressResult.NOT_IN_LIBRARY
        # The work is named, so an operator can see what they don't carry.
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == []

    def test_suppress_work_resolves_via_equivalent_identifier(
        self, db: DatabaseTransactionFixture
    ):
        """A librarian will typically have an ISBN in hand, but the
        LicensePool is often keyed on a vendor identifier (e.g. an
        Overdrive ID) with the ISBN linked only via identifier
        equivalency. The script must resolve the work through that
        equivalency instead of requiring the ISBN to be the
        LicensePool's own identifier."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)
        pool_identifier = work.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, pool_identifier, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == [test_library]

    def test_suppress_work_equivalent_identifier_only_affects_specified_library(
        self, db: DatabaseTransactionFixture
    ):
        """Resolving the work via identifier equivalency must never
        broaden *which libraries* get the work suppressed -- only the
        library explicitly passed in should end up in
        `work.suppressed_for`."""
        library_a = db.library(short_name="lib_a")
        library_b = db.library(short_name="lib_b")
        collection = db.collection(library=library_a)
        work = db.work(with_license_pool=True, collection=collection)
        pool_identifier = work.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, pool_identifier, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(library_a, isbn)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert work.suppressed_for == [library_a]
        assert library_b not in work.suppressed_for

    def test_suppress_work_ambiguous_equivalent_identifier(
        self, db: DatabaseTransactionFixture
    ):
        """If an identifier resolves to more than one distinct Work via
        equivalency, the script must not guess -- it should report
        AMBIGUOUS and suppress nothing."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, id1, 1)
        isbn.equivalent_to(source, id2, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn)

        assert result.result == SuppressResult.AMBIGUOUS
        # Candidates are ordered by work id, so the operator-facing output
        # is stable between runs on the same data.
        assert result.description == (
            f"{work1.title} (work id: {work1.id}); "
            f"{work2.title} (work id: {work2.id})"
        )
        assert work1.suppressed_for == []
        assert work2.suppressed_for == []

    def test_suppress_work_not_ambiguous_when_only_one_candidate_is_licensed_to_library(
        self, db: DatabaseTransactionFixture
    ):
        """Two different libraries each carrying "the same" title through
        their own collection (e.g. library A via OverDrive, library B via
        Bibliotheca) legitimately produces two distinct works reachable
        from one shared ISBN. Suppressing for library A alone must resolve
        cleanly to A's own work rather than reporting AMBIGUOUS over a
        candidate that isn't even licensed to A."""
        library_a = db.library(short_name="lib_a")
        library_b = db.library(short_name="lib_b")
        collection_a = db.collection(library=library_a)
        collection_b = db.collection(library=library_b)
        work_a = db.work(with_license_pool=True, collection=collection_a)
        work_b = db.work(with_license_pool=True, collection=collection_b)
        id_a = work_a.presentation_edition.primary_identifier
        id_b = work_b.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, id_a, 1)
        isbn.equivalent_to(source, id_b, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(library_a, isbn)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == f"{work_a.title} (work id: {work_a.id})"
        assert work_a.suppressed_for == [library_a]
        assert work_b.suppressed_for == []

    def test_suppress_work_work_with_pools_in_several_collections(
        self, db: DatabaseTransactionFixture
    ):
        """A Work can own pools in more than one collection (open-access
        pools are merged across collections by permanent work id). The
        library's own pool and the pool carrying the equivalent identifier
        may therefore be different pools of the same Work, so the
        collection scope has to be independent of the equivalency match
        rather than riding on the same joined row."""
        test_library = db.library(short_name="test")
        library_collection = db.collection(library=test_library)
        other_collection = db.collection()

        work = db.work(with_license_pool=True, collection=library_collection)

        # A second pool of the same work, in a collection this library
        # doesn't carry. The ISBN is equivalent to *this* pool's identifier.
        other_edition = db.edition()
        db.licensepool(other_edition, collection=other_collection, work=work)

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, other_edition.primary_identifier, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert work.suppressed_for == [test_library]

    def test_suppress_work_same_identifier_in_two_of_the_librarys_collections(
        self, db: DatabaseTransactionFixture
    ):
        """One vendor identifier can be licensed by two of a library's own
        collections -- a consortium's OverDrive collection plus that
        library's OverDrive Advantage collection, say. Each pool gets its
        own permanent Work, so suppressing one and reporting success would
        leave the title circulating through the other. Both must reach the
        ambiguity guard."""
        test_library = db.library(short_name="test")
        consortium = db.collection(library=test_library)
        advantage = db.collection(library=test_library)

        edition = db.edition()
        identifier = edition.primary_identifier

        # The same identifier, licensed separately by each collection.
        consortium_work = db.work(with_license_pool=False)
        advantage_work = db.work(with_license_pool=False)
        db.licensepool(edition, collection=consortium, work=consortium_work)
        db.licensepool(edition, collection=advantage, work=advantage_work)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, identifier)

        assert result.result == SuppressResult.AMBIGUOUS
        assert consortium_work.suppressed_for == []
        assert advantage_work.suppressed_for == []

    def test_suppress_work_same_identifier_in_two_collections_with_flag(
        self, db: DatabaseTransactionFixture
    ):
        """--suppress-ambiguous covers every collection's copy, which is
        what an operator wants once they know why there are two."""
        test_library = db.library(short_name="test")
        consortium = db.collection(library=test_library)
        advantage = db.collection(library=test_library)

        edition = db.edition()
        identifier = edition.primary_identifier

        consortium_work = db.work(with_license_pool=False)
        advantage_work = db.work(with_license_pool=False)
        db.licensepool(edition, collection=consortium, work=consortium_work)
        db.licensepool(edition, collection=advantage, work=advantage_work)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, identifier, suppress_ambiguous=True)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert consortium_work.suppressed_for == [test_library]
        assert advantage_work.suppressed_for == [test_library]

    def test_suppress_work_identifier_pool_outside_library_other_pool_inside(
        self, db: DatabaseTransactionFixture
    ):
        """The identifier's own pool may sit outside the library while the
        same Work has another pool inside it. Resolution has to find that
        work through the equivalency query -- which reaches it because an
        identifier is always a member of its own equivalent set (the
        fn_recursive_equivalents base case seeds the CTE with it)."""
        test_library = db.library(short_name="test")
        library_collection = db.collection(library=test_library)
        other_collection = db.collection()

        # The work's pool for `identifier` is in a collection the library
        # doesn't carry...
        work = db.work(with_license_pool=True, collection=other_collection)
        identifier = work.presentation_edition.primary_identifier

        # ...but the same work has another pool in a collection it does.
        inside_edition = db.edition()
        db.licensepool(inside_edition, collection=library_collection, work=work)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, identifier)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == [test_library]

    def test_suppress_work_direct_match_outside_library_falls_through(
        self, db: DatabaseTransactionFixture
    ):
        """An ISBN can be some *other* library's collection's own pool
        identifier (ISBN-keyed ODL/OPDS collections are the common case).
        That direct match isn't this library's work, so it must not be
        suppressed on this library's behalf -- and the equivalency
        fallback should still find the work this library does carry."""
        test_library = db.library(short_name="test")
        library_collection = db.collection(library=test_library)
        other_collection = db.collection()

        work = db.work(with_license_pool=True, collection=library_collection)

        # An ISBN that is another collection's own pool identifier.
        isbn_edition = db.edition(identifier_type="ISBN")
        other_work = db.work(with_license_pool=True, collection=other_collection)
        db.licensepool(isbn_edition, collection=other_collection, work=other_work)
        isbn = isbn_edition.primary_identifier

        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, work.presentation_edition.primary_identifier, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == [test_library]
        assert other_work.suppressed_for == []

    def test_suppress_work_suppress_ambiguous_suppresses_all_candidates(
        self, db: DatabaseTransactionFixture
    ):
        """With --suppress-ambiguous, an identifier resolving to multiple
        distinct works (e.g. the same ISBN licensed through more than one
        collection) should suppress the work for the library in every
        candidate, rather than refusing."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, id1, 1)
        isbn.equivalent_to(source, id2, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn, suppress_ambiguous=True)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description is not None
        parts = result.description.split("; ")
        assert f"{work1.title} (work id: {work1.id})" in parts
        assert f"{work2.title} (work id: {work2.id})" in parts
        assert work1.suppressed_for == [test_library]
        assert work2.suppressed_for == [test_library]

    def test_suppress_work_suppress_ambiguous_already_suppressed_for_all(
        self, db: DatabaseTransactionFixture
    ):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        work1.suppressed_for.append(test_library)
        work2.suppressed_for.append(test_library)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, id1, 1)
        isbn.equivalent_to(source, id2, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn, suppress_ambiguous=True)

        assert result.result == SuppressResult.ALREADY_SUPPRESSED
        # Not duplicated by a redundant append.
        assert work1.suppressed_for == [test_library]
        assert work2.suppressed_for == [test_library]

    def test_suppress_work_all_already_suppressed_reports_already_suppressed_without_flag(
        self, db: DatabaseTransactionFixture
    ):
        """Re-running against a fully-covered set of candidates (e.g.
        after an earlier --suppress-ambiguous run) must be idempotent: it
        should report ALREADY_SUPPRESSED, not AMBIGUOUS, since there's
        nothing left to decide or change even without the flag."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        work1.suppressed_for.append(test_library)
        work2.suppressed_for.append(test_library)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, id1, 1)
        isbn.equivalent_to(source, id2, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn)

        assert result.result == SuppressResult.ALREADY_SUPPRESSED
        assert work1.suppressed_for == [test_library]
        assert work2.suppressed_for == [test_library]

    def test_suppress_work_suppress_ambiguous_partial_already_suppressed(
        self, db: DatabaseTransactionFixture
    ):
        """If some but not all candidates are already suppressed, the
        overall result is NEWLY_SUPPRESSED since the run had an effect,
        every candidate ends up suppressed, and the reported title
        describes only the candidate that actually changed."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        work1.suppressed_for.append(test_library)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, id1, 1)
        isbn.equivalent_to(source, id2, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn, suppress_ambiguous=True)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert work1.suppressed_for == [test_library]
        assert work2.suppressed_for == [test_library]
        # Only the newly-changed work2 is described -- work1 was already
        # suppressed before this run, so it isn't reported as an action
        # that just happened.
        assert result.description == f"{work2.title} (work id: {work2.id})"

    def test_suppress_work_suppress_ambiguous_dry_run_does_not_mutate(
        self, db: DatabaseTransactionFixture
    ):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(source, id1, 1)
        isbn.equivalent_to(source, id2, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, isbn, dry_run=True, suppress_ambiguous=True
        )

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert work1.suppressed_for == []
        assert work2.suppressed_for == []

    def test_suppress_work_prefers_direct_match_over_ambiguous_equivalency(
        self, db: DatabaseTransactionFixture
    ):
        """An identifier that owns a LicensePool directly is an exact
        match and must win even if it's also equivalent (via messy
        metadata) to a different, unrelated Work -- equivalency should
        only be consulted as a fallback when there's no direct match,
        never used to second-guess one."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)
        identifier = work.presentation_edition.primary_identifier

        other_work = db.work(with_license_pool=True, collection=collection)
        other_identifier = other_work.presentation_edition.primary_identifier

        source = DataSource.lookup(db.session, DataSource.OCLC)
        identifier.equivalent_to(source, other_identifier, 1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, identifier)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == [test_library]
        assert other_work.suppressed_for == []

    def test_suppress_work_dry_run(self, db: DatabaseTransactionFixture):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library,
            work.presentation_edition.primary_identifier,
            dry_run=True,
        )

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert work.suppressed_for == []

    def test_suppress_work_dry_run_already_suppressed(
        self, db: DatabaseTransactionFixture
    ):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)
        work.suppressed_for.append(test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library,
            work.presentation_edition.primary_identifier,
            dry_run=True,
        )

        assert result.result == SuppressResult.ALREADY_SUPPRESSED

    def test_print_results_normal(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="mylib", name="My Library")
        script = SuppressWorkForLibraryScript(db.session)
        results = {
            ("ISBN", "111"): SuppressOutcome(
                SuppressResult.NEWLY_SUPPRESSED, "Book One"
            ),
            ("ISBN", "222"): SuppressOutcome(
                SuppressResult.ALREADY_SUPPRESSED, "Book Two"
            ),
            ("ISBN", "333"): SuppressOutcome(SuppressResult.NOT_FOUND),
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
        assert re.search(r"Newly suppressed:\s+1", out)
        assert re.search(r"Already suppressed:\s+1", out)
        assert re.search(r"Not found:\s+1", out)
        assert "[SUPPRESSED] ISBN/111 -- Book One" in out
        assert "[ALREADY SUPPRESSED] ISBN/222 -- Book Two" in out
        assert "[NOT FOUND] ISBN/333" in out
        assert "[DRY RUN]" not in out

    def test_print_results_ambiguous(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="mylib", name="My Library")
        script = SuppressWorkForLibraryScript(db.session)
        results = {
            ("ISBN", "111"): SuppressOutcome(
                SuppressResult.AMBIGUOUS, "Book One; Book Two"
            ),
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
        assert re.search(r"Ambiguous:\s+1", out)
        assert "[AMBIGUOUS] ISBN/111 -- Book One; Book Two" in out

    def test_print_results_not_in_library(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="mylib", name="My Library")
        script = SuppressWorkForLibraryScript(db.session)
        results = {
            ("ISBN", "111"): SuppressOutcome(
                SuppressResult.NOT_IN_LIBRARY, "Book One (work id: 1)"
            ),
            ("ISBN", "222"): SuppressOutcome(SuppressResult.NOT_FOUND),
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
        # The two misses are counted separately, so an operator can tell a
        # title they don't carry from an identifier that matches nothing.
        assert re.search(r"Not in this library:\s+1", out)
        assert re.search(r"Not found:\s+1", out)
        assert "[NOT IN THIS LIBRARY] ISBN/111 -- Book One (work id: 1)" in out
        assert "[NOT FOUND] ISBN/222" in out

    def test_print_results_dry_run(self, db: DatabaseTransactionFixture, capsys):
        test_library = db.library(short_name="mylib", name="My Library")
        script = SuppressWorkForLibraryScript(db.session)
        results = {
            ("ISBN", "111"): SuppressOutcome(
                SuppressResult.NEWLY_SUPPRESSED, "Book One"
            ),
            ("ISBN", "222"): SuppressOutcome(SuppressResult.NOT_FOUND),
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
        assert re.search(r"Would suppress:\s+1", out)
        assert re.search(r"Not found:\s+1", out)
        assert "[WOULD SUPPRESS] ISBN/111 -- Book One" in out
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
        assert re.search(r"Newly suppressed:\s+0", out)
        assert re.search(r"Not found:\s+1", out)
        assert "[NOT FOUND] ISBN/nonexistent-id" in out

    def test_do_run_commits_once_for_all_suppressions(
        self, db: DatabaseTransactionFixture, tmp_path, capsys
    ):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
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
        collection = db.collection(library=test_library)
        work1 = db.work(with_license_pool=True, collection=collection)
        work2 = db.work(with_license_pool=True, collection=collection)
        id1 = work1.presentation_edition.primary_identifier
        id2 = work2.presentation_edition.primary_identifier

        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(
            f"identifier,identifier_type\n"
            f"{id1.identifier},{id1.type}\n"
            f"{id2.identifier},{id2.type}\n"
        )

        script = SuppressWorkForLibraryScript(db.session)
        with (
            patch.object(db.session, "commit", side_effect=Exception("DB error")),
            patch.object(db.session, "rollback") as mock_rollback,
        ):
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
        with (
            patch.object(
                script, "suppress_work", side_effect=RuntimeError("unexpected")
            ),
            patch.object(db.session, "rollback") as mock_rollback,
        ):
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
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)

        script = SuppressWorkForLibraryScript(db.session)
        with patch.object(db.session, "commit") as mock_commit:
            result = script.suppress_work(
                test_library, work.presentation_edition.primary_identifier
            )
        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        mock_commit.assert_not_called()
