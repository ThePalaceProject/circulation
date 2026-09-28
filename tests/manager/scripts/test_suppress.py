from __future__ import annotations

import re
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import create_autospec, patch

import pytest

from palace.manager.scripts.suppress import (
    SuppressOutcome,
    SuppressResult,
    SuppressWorkForLibraryScript,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture


def isbn_equivalent_to_works(
    db: DatabaseTransactionFixture, library: Library, work_count: int = 2
) -> tuple[Identifier, list[Work]]:
    """An ISBN with no LicensePool of its own, linked by equivalency to
    `work_count` distinct works licensed to `library`.

    This is the shape a librarian hits in practice: the ISBN they have in
    hand isn't any pool's own identifier, and more than one work can hang
    off it.
    """
    collection = db.collection(library=library)
    works = [
        db.work(with_license_pool=True, collection=collection)
        for _ in range(work_count)
    ]
    isbn = db.identifier(identifier_type="ISBN")
    source = DataSource.lookup(db.session, DataSource.OCLC)
    for work in works:
        isbn.equivalent_to(source, work.presentation_edition.primary_identifier, 1)
    return isbn, works


def works_with_identifier_csv(
    db: DatabaseTransactionFixture,
    library: Library,
    tmp_path: Path,
    *,
    repeat_first: bool = False,
) -> tuple[list[Work], str]:
    """Two works licensed to `library`, plus the path to a CSV naming
    their identifiers. `repeat_first` duplicates the first row."""
    collection = db.collection(library=library)
    works = [db.work(with_license_pool=True, collection=collection) for _ in range(2)]
    identifiers = [work.presentation_edition.primary_identifier for work in works]
    if repeat_first:
        identifiers.insert(1, identifiers[0])

    csv_file = tmp_path / "ids.csv"
    csv_file.write_text(
        "identifier,identifier_type\n"
        + "".join(f"{i.identifier},{i.type}\n" for i in identifiers)
    )
    return works, str(csv_file)


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

    @pytest.mark.parametrize(
        "extra_args,attribute,expected",
        [
            pytest.param([], "dry_run", False, id="dry-run-default"),
            pytest.param(["--dry-run"], "dry_run", True, id="dry-run-set"),
            pytest.param(
                [], "suppress_ambiguous", False, id="suppress-ambiguous-default"
            ),
            pytest.param(
                ["--suppress-ambiguous"],
                "suppress_ambiguous",
                True,
                id="suppress-ambiguous-set",
            ),
        ],
    )
    def test_parse_command_line_flags(
        self,
        db: DatabaseTransactionFixture,
        extra_args: list[str],
        attribute: str,
        expected: bool,
    ):
        parsed = SuppressWorkForLibraryScript.parse_command_line(
            db.session,
            ["--library", "lib1", "--identifier", "123", *extra_args],
        )
        assert getattr(parsed, attribute) is expected

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

    @pytest.mark.parametrize(
        "csv_content,expected",
        [
            pytest.param(
                """\
                identifier,identifier_type
                978-0-06-112008-4,ISBN
                12345,Overdrive ID
                ,ISBN
                """,
                [("ISBN", "978-0-06-112008-4"), ("Overdrive ID", "12345")],
                id="row-without-an-identifier-is-skipped",
            ),
            pytest.param(
                """\
                identifier
                978-0-06-112008-4
                12345
                """,
                [("ISBN", "978-0-06-112008-4"), ("ISBN", "12345")],
                id="no-type-column-falls-back-to-default",
            ),
            pytest.param(
                # A row that omits the trailing comma entirely ('12345' rather
                # than '12345,') leaves DictReader with None, not "", so the
                # fallback must not call .strip() on it.
                """\
                identifier,identifier_type
                978-0-06-112008-4
                12345,Overdrive ID
                """,
                [("ISBN", "978-0-06-112008-4"), ("Overdrive ID", "12345")],
                id="omitted-type-value-falls-back-to-default",
            ),
            pytest.param(
                """\
                identifier,identifier_type
                978-0-06-112008-4,
                12345,Overdrive ID
                """,
                [("ISBN", "978-0-06-112008-4"), ("Overdrive ID", "12345")],
                id="empty-type-value-falls-back-to-default",
            ),
            pytest.param(
                """\
                identifier,identifier_type
                978-0-06-112008-4,ISBN
                978-0-06-112008-4,ISBN
                """,
                [("ISBN", "978-0-06-112008-4"), ("ISBN", "978-0-06-112008-4")],
                id="duplicates-are-preserved-for-do-run-to-dedupe",
            ),
        ],
    )
    def test_load_identifiers_from_file(
        self,
        db: DatabaseTransactionFixture,
        tmp_path,
        csv_content: str,
        expected: list[tuple[str, str]],
    ):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text(textwrap.dedent(csv_content))

        script = SuppressWorkForLibraryScript(db.session)

        assert script.load_identifiers_from_file(str(csv_file), "ISBN") == expected

    def test_do_run_deduplicates_and_warns(
        self, db: DatabaseTransactionFixture, tmp_path, caplog
    ):
        """When CSV contains duplicate identifiers, they are deduplicated before
        processing and a warning is logged."""
        import logging

        test_library = db.library(short_name="test")
        works, csv_path = works_with_identifier_csv(
            db, test_library, tmp_path, repeat_first=True
        )

        caplog.set_level(logging.WARNING)
        script = SuppressWorkForLibraryScript(db.session)
        script.do_run(["--library", test_library.short_name, "--file", csv_path])

        assert "Removed 1 duplicate identifier(s) from input" in caplog.text
        for work in works:
            assert test_library in work.suppressed_for

    def test_load_identifiers_from_file_missing_identifier_column(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text("foo,bar\n1,2\n")

        script = SuppressWorkForLibraryScript(db.session)
        with pytest.raises(ValueError, match='must contain an "identifier" column'):
            script.load_identifiers_from_file(str(csv_file), "ISBN")

    @pytest.mark.parametrize(
        "extra_args,expected_kwargs",
        [
            pytest.param(
                [], {"dry_run": False, "suppress_ambiguous": False}, id="no-flags"
            ),
            pytest.param(
                ["--dry-run"],
                {"dry_run": True, "suppress_ambiguous": False},
                id="dry-run",
            ),
            pytest.param(
                ["--suppress-ambiguous"],
                {"dry_run": False, "suppress_ambiguous": True},
                id="suppress-ambiguous",
            ),
        ],
    )
    def test_do_run_passes_flags_to_suppress_work(
        self,
        db: DatabaseTransactionFixture,
        capsys,
        extra_args: list[str],
        expected_kwargs: dict[str, bool],
    ):
        test_library = db.library(short_name="test")
        test_identifier = db.identifier()

        script = SuppressWorkForLibraryScript(db.session)
        suppress_work_mock = create_autospec(script.suppress_work)
        suppress_work_mock.return_value = SuppressOutcome(
            SuppressResult.NEWLY_SUPPRESSED, "Some Title"
        )
        script.suppress_work = suppress_work_mock

        script.do_run(
            [
                "--library",
                test_library.short_name,
                "--identifier-type",
                test_identifier.type,
                "--identifier",
                test_identifier.identifier,
                *extra_args,
            ]
        )

        suppress_work_mock.assert_called_once_with(
            test_library, test_identifier, **expected_kwargs
        )

    def test_do_run_with_file(self, db: DatabaseTransactionFixture, tmp_path, capsys):
        test_library = db.library(short_name="test")
        works, csv_path = works_with_identifier_csv(db, test_library, tmp_path)

        script = SuppressWorkForLibraryScript(db.session)
        script.do_run(["--library", test_library.short_name, "--file", csv_path])

        for work in works:
            assert test_library in work.suppressed_for

        out = capsys.readouterr().out
        assert re.search(r"Newly suppressed:\s+2", out)
        assert re.search(r"Already suppressed:\s+0", out)
        assert re.search(r"Not found:\s+0", out)

    @pytest.mark.parametrize(
        "already_suppressed,dry_run,expected_result,suppressed_after",
        [
            pytest.param(
                False, False, SuppressResult.NEWLY_SUPPRESSED, True, id="suppresses"
            ),
            pytest.param(
                True,
                False,
                SuppressResult.ALREADY_SUPPRESSED,
                True,
                id="already-suppressed",
            ),
            pytest.param(
                False, True, SuppressResult.NEWLY_SUPPRESSED, False, id="dry-run"
            ),
            pytest.param(
                True,
                True,
                SuppressResult.ALREADY_SUPPRESSED,
                True,
                id="dry-run-already-suppressed",
            ),
        ],
    )
    def test_suppress_work(
        self,
        db: DatabaseTransactionFixture,
        already_suppressed: bool,
        dry_run: bool,
        expected_result: SuppressResult,
        suppressed_after: bool,
    ):
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)
        if already_suppressed:
            work.suppressed_for.append(test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library,
            work.presentation_edition.primary_identifier,
            dry_run=dry_run,
        )

        assert result.result == expected_result
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == ([test_library] if suppressed_after else [])

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
        isbn, (work,) = isbn_equivalent_to_works(db, test_library, work_count=1)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == f"{work.title} (work id: {work.id})"
        assert work.suppressed_for == [test_library]

    @pytest.mark.parametrize(
        "strength,expected_result,expect_suppressed",
        [
            pytest.param(
                1, SuppressResult.NEWLY_SUPPRESSED, True, id="full-confidence-resolves"
            ),
            pytest.param(
                0.85, SuppressResult.NOT_FOUND, False, id="below-threshold-ignored"
            ),
        ],
    )
    def test_suppress_work_only_high_confidence_equivalencies_resolve(
        self,
        db: DatabaseTransactionFixture,
        strength: float,
        expected_result: SuppressResult,
        expect_suppressed: bool,
    ):
        """Equivalency resolution uses `Work.from_identifiers`' strict
        default policy (threshold 0.999), so only assertions a data source
        is fully confident about can pull a work into a suppression.

        0.85 isn't an arbitrary "low" number: it's the strength the
        importer itself assigns when it links two identifiers purely
        because their editions share a permanent work id
        (`BibliographicData` in `data_layer/bibliographic.py`). Those
        edges exist throughout production data, and a suppression must
        not ride one into a work the librarian never named."""
        test_library = db.library(short_name="test")
        collection = db.collection(library=test_library)
        work = db.work(with_license_pool=True, collection=collection)

        isbn = db.identifier(identifier_type="ISBN")
        source = DataSource.lookup(db.session, DataSource.OCLC)
        isbn.equivalent_to(
            source, work.presentation_edition.primary_identifier, strength
        )

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn)

        assert result.result == expected_result
        assert work.suppressed_for == ([test_library] if expect_suppressed else [])

    def test_suppress_work_equivalent_identifier_only_affects_specified_library(
        self, db: DatabaseTransactionFixture
    ):
        """Resolving the work via identifier equivalency must never
        broaden *which libraries* get the work suppressed -- only the
        library explicitly passed in should end up in
        `work.suppressed_for`."""
        library_a = db.library(short_name="lib_a")
        library_b = db.library(short_name="lib_b")
        isbn, (work,) = isbn_equivalent_to_works(db, library_a, work_count=1)

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
        isbn, (work1, work2) = isbn_equivalent_to_works(db, test_library)

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

    @pytest.mark.parametrize(
        "suppress_ambiguous,expected_result,suppressed_after",
        [
            pytest.param(
                False, SuppressResult.AMBIGUOUS, False, id="refuses-without-flag"
            ),
            pytest.param(
                True, SuppressResult.NEWLY_SUPPRESSED, True, id="covers-both-with-flag"
            ),
        ],
    )
    def test_suppress_work_same_identifier_in_two_of_the_librarys_collections(
        self,
        db: DatabaseTransactionFixture,
        suppress_ambiguous: bool,
        expected_result: SuppressResult,
        suppressed_after: bool,
    ):
        """One vendor identifier can be licensed by two of a library's own
        collections -- a consortium's OverDrive collection plus that
        library's OverDrive Advantage collection, say. Each pool gets its
        own permanent Work, so suppressing one and reporting success would
        leave the title circulating through the other: both must reach the
        ambiguity guard, and --suppress-ambiguous must cover both."""
        test_library = db.library(short_name="test")
        edition = db.edition()

        # The same identifier, licensed separately by each collection.
        works = []
        for _ in range(2):
            work = db.work(with_license_pool=False)
            db.licensepool(
                edition, collection=db.collection(library=test_library), work=work
            )
            works.append(work)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library,
            edition.primary_identifier,
            suppress_ambiguous=suppress_ambiguous,
        )

        assert result.result == expected_result
        for work in works:
            assert work.suppressed_for == ([test_library] if suppressed_after else [])

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
        isbn, (work1, work2) = isbn_equivalent_to_works(db, test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(test_library, isbn, suppress_ambiguous=True)

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        assert result.description == (
            f"{work1.title} (work id: {work1.id}); "
            f"{work2.title} (work id: {work2.id})"
        )
        assert work1.suppressed_for == [test_library]
        assert work2.suppressed_for == [test_library]

    @pytest.mark.parametrize(
        "suppress_ambiguous",
        [pytest.param(False, id="without-flag"), pytest.param(True, id="with-flag")],
    )
    def test_suppress_work_all_candidates_already_suppressed(
        self, db: DatabaseTransactionFixture, suppress_ambiguous: bool
    ):
        """Re-running against a fully-covered set of candidates (e.g.
        after an earlier --suppress-ambiguous run) must be idempotent: it
        should report ALREADY_SUPPRESSED rather than AMBIGUOUS whether or
        not the flag is passed, since there's nothing left to decide or
        change -- and no duplicate rows from a redundant append."""
        test_library = db.library(short_name="test")
        isbn, works = isbn_equivalent_to_works(db, test_library)
        for work in works:
            work.suppressed_for.append(test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, isbn, suppress_ambiguous=suppress_ambiguous
        )

        assert result.result == SuppressResult.ALREADY_SUPPRESSED
        for work in works:
            assert work.suppressed_for == [test_library]

    def test_suppress_work_suppress_ambiguous_partial_already_suppressed(
        self, db: DatabaseTransactionFixture
    ):
        """If some but not all candidates are already suppressed, the
        overall result is NEWLY_SUPPRESSED since the run had an effect,
        every candidate ends up suppressed, and the reported title
        describes only the candidate that actually changed."""
        test_library = db.library(short_name="test")
        isbn, (work1, work2) = isbn_equivalent_to_works(db, test_library)
        work1.suppressed_for.append(test_library)

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
        isbn, works = isbn_equivalent_to_works(db, test_library)

        script = SuppressWorkForLibraryScript(db.session)
        result = script.suppress_work(
            test_library, isbn, dry_run=True, suppress_ambiguous=True
        )

        assert result.result == SuppressResult.NEWLY_SUPPRESSED
        for work in works:
            assert work.suppressed_for == []

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

    @pytest.mark.parametrize(
        "dry_run,results,expected,absent",
        [
            pytest.param(
                False,
                {
                    ("ISBN", "111"): SuppressOutcome(
                        SuppressResult.NEWLY_SUPPRESSED, "Book One"
                    ),
                    ("ISBN", "222"): SuppressOutcome(
                        SuppressResult.ALREADY_SUPPRESSED, "Book Two"
                    ),
                    ("ISBN", "333"): SuppressOutcome(SuppressResult.NOT_FOUND),
                },
                [
                    "Suppression Results Summary",
                    "My Library (mylib)",
                    "2026-02-26 12:00:00 UTC",
                    "1.23s",
                    "Newly suppressed: 1",
                    "Already suppressed: 1",
                    "Not found: 1",
                    "[SUPPRESSED] ISBN/111 -- Book One",
                    "[ALREADY SUPPRESSED] ISBN/222 -- Book Two",
                    "[NOT FOUND] ISBN/333",
                ],
                ["[DRY RUN]"],
                id="normal",
            ),
            pytest.param(
                True,
                {
                    ("ISBN", "111"): SuppressOutcome(
                        SuppressResult.NEWLY_SUPPRESSED, "Book One"
                    ),
                    ("ISBN", "222"): SuppressOutcome(SuppressResult.NOT_FOUND),
                },
                [
                    "[DRY RUN] Suppression Results Summary",
                    "Would suppress: 1",
                    "Not found: 1",
                    "[WOULD SUPPRESS] ISBN/111 -- Book One",
                    "[NOT FOUND] ISBN/222",
                ],
                ["[SUPPRESSED]"],
                id="dry-run",
            ),
            pytest.param(
                False,
                {
                    ("ISBN", "111"): SuppressOutcome(
                        SuppressResult.AMBIGUOUS, "Book One; Book Two"
                    ),
                },
                [
                    "Ambiguous: 1",
                    "[AMBIGUOUS] ISBN/111 -- Book One; Book Two",
                ],
                [],
                id="ambiguous",
            ),
            pytest.param(
                False,
                {
                    ("ISBN", "111"): SuppressOutcome(
                        SuppressResult.NOT_IN_LIBRARY, "Book One (work id: 1)"
                    ),
                    ("ISBN", "222"): SuppressOutcome(SuppressResult.NOT_FOUND),
                },
                # The two misses are counted separately, so an operator can
                # tell a title they don't carry from an identifier that
                # matches nothing.
                [
                    "Not in this library: 1",
                    "Not found: 1",
                    "[NOT IN THIS LIBRARY] ISBN/111 -- Book One (work id: 1)",
                    "[NOT FOUND] ISBN/222",
                ],
                [],
                id="not-in-library",
            ),
        ],
    )
    def test_print_results(
        self,
        db: DatabaseTransactionFixture,
        capsys,
        dry_run: bool,
        results: dict[tuple[str, str], SuppressOutcome],
        expected: list[str],
        absent: list[str],
    ):
        test_library = db.library(short_name="mylib", name="My Library")
        script = SuppressWorkForLibraryScript(db.session)

        script._print_results(
            results,
            dry_run=dry_run,
            library=test_library,
            started_at=datetime(2026, 2, 26, 12, 0, 0, tzinfo=timezone.utc),
            duration_seconds=1.23,
        )

        # Summary rows are column-padded, so compare against a
        # whitespace-collapsed copy to keep the expectations readable.
        out = re.sub(r"\s+", " ", capsys.readouterr().out)
        for fragment in expected:
            assert fragment in out
        for fragment in absent:
            assert fragment not in out

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
        works, csv_path = works_with_identifier_csv(db, test_library, tmp_path)

        script = SuppressWorkForLibraryScript(db.session)
        with patch.object(db.session, "commit", wraps=db.session.commit) as mock_commit:
            script.do_run(["--library", test_library.short_name, "--file", csv_path])
            mock_commit.assert_called_once()

        for work in works:
            assert test_library in work.suppressed_for

    def test_do_run_rolls_back_all_on_commit_failure(
        self, db: DatabaseTransactionFixture, tmp_path
    ):
        test_library = db.library(short_name="test")
        _, csv_path = works_with_identifier_csv(db, test_library, tmp_path)

        script = SuppressWorkForLibraryScript(db.session)
        with (
            patch.object(db.session, "commit", side_effect=Exception("DB error")),
            patch.object(db.session, "rollback") as mock_rollback,
        ):
            with pytest.raises(Exception, match="DB error"):
                script.do_run(
                    ["--library", test_library.short_name, "--file", csv_path]
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
