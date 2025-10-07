from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.reporting.tables.library_all_title import LibraryAllTitleReportTable
from tests.fixtures.database import DatabaseTransactionFixture


class TestLibraryAllTitleReportTable:

    EXPECTED_HEADINGS = (
        "title",
        "author",
        "identifier_type",
        "identifier",
        "isbn",
        "language",
        "publisher",
        "format",
        "audience",
        "genres",
        "data_source",
        "collection",
    )

    def test_definition(self, db: DatabaseTransactionFixture):
        library = db.default_library()
        table = LibraryAllTitleReportTable(session=db.session, library_id=library.id)

        assert table.definition == LibraryAllTitleReportTable.DEFINITION
        assert table.headings == self.EXPECTED_HEADINGS

        assert LibraryAllTitleReportTable.DEFINITION.key == "all-title"
        assert LibraryAllTitleReportTable.DEFINITION.title == "All Title"
        assert LibraryAllTitleReportTable.DEFINITION.headings == self.EXPECTED_HEADINGS

    def test_call(self, db: DatabaseTransactionFixture):
        library = db.default_library()

        # Set up a mock table processor.
        table_processor = MagicMock()
        table_processor.return_value = True

        # Instantiate the table and call it with the processor.
        table = LibraryAllTitleReportTable(session=db.session, library_id=library.id)
        result = table(table_processor)

        # The processor should've been called with the table's rows and headings.
        table_processor_kwargs = table_processor.call_args.kwargs
        assert len(table_processor.call_args.args) == 0
        assert len(table_processor_kwargs) == 2
        assert isinstance(table_processor_kwargs["rows"], Generator)
        assert table_processor_kwargs["headings"] == self.EXPECTED_HEADINGS
        assert result == table_processor.return_value

        # An exception in the table processor should propagate.
        table_processor.side_effect = Exception("Something went wrong")
        with pytest.raises(Exception, match="Something went wrong"):
            table(table_processor)

    def test_included_collections(self, db: DatabaseTransactionFixture):
        library = db.default_library()
        active_collection = db.default_collection()
        inactive_collection = db.default_inactive_collection()

        # We cannot directly patch the row function on the table definition,
        # since it is immutable. So, we patch the statement and the session's
        # execute method to get a view into what is happening.
        def check_params(table, integration_ids):
            with patch.object(
                LibraryAllTitleReportTable.DEFINITION.statement, "params"
            ) as mock_params:
                mock_statement = MagicMock()
                mock_params.return_value = mock_statement

                with patch.object(table.session, "execute") as mock_execute:
                    mock_execute.return_value = iter([])
                    # This is the actual call we're testing.
                    list(table.rows)
                    mock_params.assert_called_once()
                    # Check that the integration_ids match, regardless of order.
                    actual_integration_ids = mock_params.call_args.kwargs[
                        "integration_ids"
                    ]
                    assert set(actual_integration_ids) == set(integration_ids)

                    mock_execute.assert_called_once_with(mock_statement)

        # If no collection ids are specified, the library's active collections are included.
        assert LibraryAllTitleReportTable.included_collections(
            session=db.session, library_id=library.id
        ) == [active_collection]

        table = LibraryAllTitleReportTable(session=db.session, library_id=library.id)
        assert table.included_collections(
            session=db.session, library_id=library.id
        ) == [active_collection]

        check_params(
            table, integration_ids=[active_collection.integration_configuration.id]
        )

        # Inactive collections can be included by explicitly specifying them.
        test_collection_ids = [inactive_collection.id]

        assert LibraryAllTitleReportTable.included_collections(
            session=db.session,
            library_id=library.id,
            collection_ids=test_collection_ids,
        ) == [inactive_collection]

        table = LibraryAllTitleReportTable(
            session=db.session,
            library_id=library.id,
            collection_ids=test_collection_ids,
        )
        assert table.included_collections(
            session=db.session,
            library_id=library.id,
            collection_ids=test_collection_ids,
        ) == [inactive_collection]

        check_params(
            table, integration_ids=[inactive_collection.integration_configuration.id]
        )

        # Of course, an active collection may also be specified explicitly.
        test_collection_ids = [inactive_collection.id, active_collection.id]

        assert set(
            LibraryAllTitleReportTable.included_collections(
                session=db.session,
                library_id=library.id,
                collection_ids=test_collection_ids,
            )
        ) == {active_collection, inactive_collection}

        table = LibraryAllTitleReportTable(
            session=db.session,
            library_id=library.id,
            collection_ids=test_collection_ids,
        )
        assert set(
            table.included_collections(
                session=db.session,
                library_id=library.id,
                collection_ids=test_collection_ids,
            )
        ) == {active_collection, inactive_collection}

        check_params(
            table,
            integration_ids=[
                active_collection.integration_configuration.id,
                inactive_collection.integration_configuration.id,
            ],
        )

        # If called with a collection not associated with the library,
        # we get a PalaceValueError.
        # To test, we'll conjure an id that shouldn't exist.
        invalid_collection_id = active_collection.id + inactive_collection.id
        test_collection_ids = [invalid_collection_id]
        with pytest.raises(
            PalaceValueError,
            match=rf"Ineligible report collection id\(s\) for library '{library.name}' \(id={library.id}\): {invalid_collection_id}",
        ):
            LibraryAllTitleReportTable.included_collections(
                session=db.session,
                library_id=library.id,
                collection_ids=test_collection_ids,
            )
