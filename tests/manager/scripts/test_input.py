from __future__ import annotations

import pytest

from palace.manager.scripts.input import (
    CollectionArgumentsScript,
    CollectionInputScript,
    IdentifierInputScript,
    LibraryInputScript,
    PatronInputScript,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.stdin import MockStdin


class TestIdentifierInputScript:
    def test_parse_list_as_identifiers(self, db: DatabaseTransactionFixture):
        i1 = db.identifier()
        i2 = db.identifier()
        args = [i1.identifier, "no-such-identifier", i2.identifier]
        identifiers = IdentifierInputScript.parse_identifier_list(
            db.session, i1.type, None, args
        )
        assert [i1, i2] == identifiers

        assert [] == IdentifierInputScript.parse_identifier_list(
            db.session, i1.type, None, []
        )

    def test_parse_list_as_identifiers_with_autocreate(
        self, db: DatabaseTransactionFixture
    ):
        type = Identifier.OVERDRIVE_ID
        args = ["brand-new-identifier"]
        [i] = IdentifierInputScript.parse_identifier_list(
            db.session, type, None, args, autocreate=True
        )
        assert type == i.type
        assert "brand-new-identifier" == i.identifier

    def test_parse_list_as_identifiers_with_data_source(
        self, db: DatabaseTransactionFixture
    ):
        lp1 = db.licensepool(None, data_source_name=DataSource.UNGLUE_IT)
        lp2 = db.licensepool(None, data_source_name=DataSource.FEEDBOOKS)
        lp3 = db.licensepool(None, data_source_name=DataSource.FEEDBOOKS)

        i1, i2, i3 = (lp.identifier for lp in [lp1, lp2, lp3])
        i1.type = i2.type = Identifier.URI
        source = DataSource.lookup(db.session, DataSource.FEEDBOOKS)

        # Only URIs with a FeedBooks LicensePool are selected.
        identifiers = IdentifierInputScript.parse_identifier_list(
            db.session, Identifier.URI, source, []
        )
        assert [i2] == identifiers

    def test_parse_list_as_identifiers_by_database_id(
        self, db: DatabaseTransactionFixture
    ):
        id1 = db.identifier()
        id2 = db.identifier()

        # Make a list containing two Identifier database IDs,
        # as well as two strings which are not existing Identifier database
        # IDs.
        ids = [id1.id, "10000000", "abcde", id2.id]

        identifiers = IdentifierInputScript.parse_identifier_list(
            db.session, IdentifierInputScript.DATABASE_ID, None, ids
        )
        assert [id1, id2] == identifiers

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        i1 = db.identifier()
        i2 = db.identifier()
        # We pass in one identifier on the command line...
        cmd_args = ["--identifier-type", i1.type, i1.identifier]
        # ...and another one into standard input.
        stdin = MockStdin(i2.identifier)
        parsed = IdentifierInputScript.parse_command_line(db.session, cmd_args, stdin)
        assert [i1, i2] == parsed.identifiers
        assert i1.type == parsed.identifier_type

    def test_parse_command_line_no_identifiers(self, db: DatabaseTransactionFixture):
        cmd_args = [
            "--identifier-type",
            Identifier.OVERDRIVE_ID,
            "--identifier-data-source",
            DataSource.STANDARD_EBOOKS,
        ]
        parsed = IdentifierInputScript.parse_command_line(
            db.session, cmd_args, MockStdin()
        )
        assert [] == parsed.identifiers
        assert Identifier.OVERDRIVE_ID == parsed.identifier_type
        assert DataSource.STANDARD_EBOOKS == parsed.identifier_data_source


class TestPatronInputScript:
    def test_parse_patron_list(self, db: DatabaseTransactionFixture):
        """Test that patrons can be identified with any unique identifier."""
        l1 = db.library()
        l2 = db.library()
        p1 = db.patron()
        p1.authorization_identifier = db.fresh_str()
        p1.library_id = l1.id
        p2 = db.patron()
        p2.username = db.fresh_str()
        p2.library_id = l1.id
        p3 = db.patron()
        p3.external_identifier = db.fresh_str()
        p3.library_id = l1.id
        p4 = db.patron()
        p4.external_identifier = db.fresh_str()
        p4.library_id = l2.id
        args = [
            p1.authorization_identifier,
            "no-such-patron",
            "",
            p2.username,
            p3.external_identifier,
        ]
        patrons = PatronInputScript.parse_patron_list(db.session, l1, args)
        assert [p1, p2, p3] == patrons
        assert [] == PatronInputScript.parse_patron_list(db.session, l1, [])
        assert [p1] == PatronInputScript.parse_patron_list(
            db.session, l1, [p1.external_identifier, p4.external_identifier]
        )
        assert [p4] == PatronInputScript.parse_patron_list(
            db.session, l2, [p1.external_identifier, p4.external_identifier]
        )

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        l1 = db.library()
        p1 = db.patron()
        p2 = db.patron()
        p1.authorization_identifier = db.fresh_str()
        p2.authorization_identifier = db.fresh_str()
        p1.library_id = l1.id
        p2.library_id = l1.id
        # We pass in one patron identifier on the command line...
        cmd_args = [l1.short_name, p1.authorization_identifier]
        # ...and another one into standard input.
        stdin = MockStdin(p2.authorization_identifier)
        parsed = PatronInputScript.parse_command_line(db.session, cmd_args, stdin)
        assert [p1, p2] == parsed.patrons

    def test_patron_different_library(self, db: DatabaseTransactionFixture):
        l1 = db.library()
        l2 = db.library()
        p1 = db.patron()
        p2 = db.patron()
        p1.authorization_identifier = db.fresh_str()
        p2.authorization_identifier = p1.authorization_identifier
        p1.library_id = l1.id
        p2.library_id = l2.id
        cmd_args = [l1.short_name, p1.authorization_identifier]
        parsed = PatronInputScript.parse_command_line(db.session, cmd_args, None)
        assert [p1] == parsed.patrons
        cmd_args = [l2.short_name, p2.authorization_identifier]
        parsed = PatronInputScript.parse_command_line(db.session, cmd_args, None)
        assert [p2] == parsed.patrons

    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that PatronInputScript.do_run() calls process_patron()
        for every patron designated by the command-line arguments.
        """

        processed_patrons = []

        class MockPatronInputScript(PatronInputScript):
            def process_patron(self, patron):
                processed_patrons.append(patron)

        l1 = db.library()
        p1 = db.patron()
        p2 = db.patron()
        p3 = db.patron()
        p1.library_id = l1.id
        p2.library_id = l1.id
        p3.library_id = l1.id
        p1.authorization_identifier = db.fresh_str()
        p2.authorization_identifier = db.fresh_str()
        cmd_args = [l1.short_name, p1.authorization_identifier]
        stdin = MockStdin(p2.authorization_identifier)
        script = MockPatronInputScript(db.session)
        script.do_run(cmd_args=cmd_args, stdin=stdin)
        assert p1 in processed_patrons
        assert p2 in processed_patrons
        assert p3 not in processed_patrons


class TestLibraryInputScript:
    def test_parse_library_list(self, db: DatabaseTransactionFixture):
        """Test that libraries can be identified with their full name or short name."""
        l1 = db.library()
        l2 = db.library()
        args = [l1.name, "no-such-library", "", l2.short_name]
        libraries = LibraryInputScript.parse_library_list(db.session, args)
        assert [l1, l2] == libraries

        assert [] == LibraryInputScript.parse_library_list(db.session, [])

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        l1 = db.library()
        # We pass in one library identifier on the command line...
        cmd_args = [l1.name]
        parsed = LibraryInputScript.parse_command_line(db.session, cmd_args)

        # And here it is.
        assert [l1] == parsed.libraries

    def test_parse_command_line_no_identifiers(self, db: DatabaseTransactionFixture):
        """If you don't specify any libraries on the command
        line, we will process all libraries in the system.
        """
        parsed = LibraryInputScript.parse_command_line(db.session, [])
        assert db.session.query(Library).all() == parsed.libraries

    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that LibraryInputScript.do_run() calls process_library()
        for every library designated by the command-line arguments.
        """

        processed_libraries = []

        class MockLibraryInputScript(LibraryInputScript):
            def process_library(self, library):
                processed_libraries.append(library)

        l1 = db.library()
        l2 = db.library()
        cmd_args = [l1.name]
        script = MockLibraryInputScript(db.session)
        script.do_run(cmd_args=cmd_args)
        assert l1 in processed_libraries
        assert l2 not in processed_libraries


class TestCollectionInputScript:
    """Test the ability to name collections on the command line."""

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        def collections(cmd_args):
            parsed = CollectionInputScript.parse_command_line(db.session, cmd_args)
            return parsed.collections

        # No collections named on command line -> no collections
        assert [] == collections([])

        # Nonexistent collection -> ValueError
        with pytest.raises(ValueError) as excinfo:
            collections(['--collection="no such collection"'])
        assert 'Unknown collection: "no such collection"' in str(excinfo.value)

        # Collections are presented in the order they were encountered
        # on the command line.
        c2 = db.collection()
        expect = [c2, db.default_collection()]
        args = ["--collection=" + c.name for c in expect]
        actual = collections(args)
        assert expect == actual


class TestCollectionArgumentsScript:
    """Test the ability to take collection arguments on the command line."""

    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        def collections(cmd_args):
            parsed = CollectionArgumentsScript.parse_command_line(db.session, cmd_args)
            return parsed.collections

        # No collections named on command line -> no collections
        assert [] == collections([])

        # Nonexistent collection -> ValueError
        with pytest.raises(ValueError) as excinfo:
            collections(["no such collection"])
        assert "Unknown collection: no such collection" in str(excinfo.value)

        # Collections are presented in the order they were encountered
        # on the command line.
        c2 = db.collection()
        expect = [c2, db.default_collection()]
        args = [c.name for c in expect]
        actual = collections(args)
        assert expect == actual

        # It is okay to not specify any collections.
        expect = []
        args = [c.name for c in expect]
        actual = collections(args)
        assert expect == actual
