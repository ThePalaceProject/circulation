from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy import Integer, String, bindparam, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.reporting.model import TabularQueryDefinition
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from tests.fixtures.database import DatabaseTransactionFixture


class TestTabularQueryDefinition:

    @pytest.fixture
    def mock_statement(self) -> MagicMock:
        """Creates a mock SQLAlchemy Select statement."""
        statement = MagicMock(spec=Select)

        col_a = Mock()
        col_a.name = "Column A"
        col_b = Mock()
        col_b.name = "Column B"
        columns = [col_a, col_b]
        statement.c = columns

        mock_parameterized_statement = MagicMock(spec=Select)
        statement.params.return_value = mock_parameterized_statement

        return statement

    @pytest.fixture
    def mock_session(self) -> MagicMock:
        """Creates a mock SQLAlchemy Session."""
        return MagicMock(spec=Session)

    @pytest.mark.parametrize(
        "column_names,expected_headings",
        [
            pytest.param(
                ["id", "title", "author"],
                ("id", "title", "author"),
                id="multiple-columns",
            ),
            pytest.param(["single_column"], ("single_column",), id="single-column"),
            pytest.param(
                ["Column with Spaces", "another-column"],
                ("Column with Spaces", "another-column"),
                id="column-with-spaces",
            ),
        ],
    )
    def test_headings(
        self,
        mock_statement: MagicMock,
        column_names: list[str],
        expected_headings: tuple[str, ...],
    ):
        statement = MagicMock(spec=Select)
        mock_columns = []
        for name in column_names:
            mock_col = Mock()
            mock_col.name = name
            mock_columns.append(mock_col)
        statement.c = mock_columns
        statement.params.return_value = statement

        definition = TabularQueryDefinition(
            key="test-key", title="Test Title", statement=statement
        )

        assert definition.headings == expected_headings

    @pytest.mark.parametrize(
        "query_data",
        [
            pytest.param([], id="empty-results"),
            pytest.param([(1, "Alice"), (2, "Bob")], id="basic-rows"),
            pytest.param([(1, "Test", True, 3.14)], id="mixed-types"),
            pytest.param([(None, ""), (0, False)], id="null-and-falsy-values"),
        ],
    )
    def test_rows_data(
        self,
        mock_statement: MagicMock,
        mock_session: MagicMock,
        query_data: list[tuple],
    ):
        mock_result = MagicMock()
        mock_result.__iter__.return_value = iter(query_data)
        mock_session.execute.return_value = mock_result

        expected_results = query_data

        definition = TabularQueryDefinition(
            key="test-rows", title="Test Rows", statement=mock_statement
        )

        result_generator = definition.rows(session=mock_session)
        assert isinstance(result_generator, Generator)
        # Consume the iterator to ensure that the statement is executed.
        results = list(result_generator)
        assert results == expected_results
        assert all(isinstance(row, tuple) for row in results)

    @pytest.mark.parametrize(
        "params",
        [
            pytest.param({}, id="no-params"),
            pytest.param({"id": 1}, id="single-param"),
            pytest.param(
                {"title": "Book of Hours", "status": "active"}, id="multiple-params"
            ),
        ],
    )
    def test_rows_params(
        self, mock_statement: MagicMock, mock_session: MagicMock, params: dict[str, Any]
    ):
        mock_result = MagicMock()
        mock_session.execute.return_value = mock_result

        definition = TabularQueryDefinition(
            key="test-params", title="Test Params", statement=mock_statement
        )
        # Consume the iterator to ensure that the statement is executed.
        list(definition.rows(session=mock_session, **params))

        mock_statement.params.assert_called_once_with(**params)
        mock_session.execute.assert_called_once_with(mock_statement.params.return_value)

    def test_rows_errors_propagate(
        self, mock_statement: MagicMock, mock_session: MagicMock
    ):
        definition = TabularQueryDefinition(
            key="error-test", title="Error Test", statement=mock_statement
        )

        mock_session.execute.side_effect = Exception("Database connection failed")
        with pytest.raises(Exception, match="Database connection failed"):
            list(definition.rows(session=mock_session))

        mock_statement.params.side_effect = Exception("Invalid parameters")
        with pytest.raises(Exception, match="Invalid parameters"):
            list(definition.rows(session=mock_session))

    def test_with_orm_select(self, db: DatabaseTransactionFixture):
        _l = db.default_library()
        active = db.default_collection()
        inactive = db.default_inactive_collection()

        # Set up an ORM query with a bound parameter.
        statement = (
            select(Collection.id, IntegrationConfiguration.name)
            .join(
                IntegrationConfiguration,
                Collection.integration_configuration_id == IntegrationConfiguration.id,
            )
            .where(IntegrationConfiguration.name == bindparam("collection_name"))
        )
        definition = TabularQueryDefinition(
            key="real-data",
            title="Query with Real Data",
            statement=statement,
        )

        # We can get the headings without executing the query.
        assert definition.headings == ("id", "name")

        # Request the rows using the bound parameter.
        rows1 = list(
            definition.rows(
                session=db.session, collection_name="Default Inactive Collection"
            )
        )
        rows2 = list(
            definition.rows(session=db.session, collection_name="Default Collection")
        )

        # Verify the correct rows are returned.
        assert rows1 == [(inactive.id, inactive.integration_configuration.name)]
        assert rows2 == [(active.id, active.integration_configuration.name)]

    def test_with_non_orm_select(self, db: DatabaseTransactionFixture):
        # Ensure there is real data to query against.
        _l = db.default_library()
        active = db.default_collection()
        inactive = db.default_inactive_collection()

        from sqlalchemy import text

        # Set up a raw SQL query with a bound parameter and columns.
        statement_without_columns = text(
            """
            SELECT c.id AS id, integration_configurations.name AS name
            FROM collections c
            JOIN integration_configurations
              ON c.integration_configuration_id = integration_configurations.id
            WHERE integration_configurations.name = :collection_name
            """
        )

        definition_key = "real-data"
        definition_title = "Query with Real Data"

        no_columns = TabularQueryDefinition(
            key=definition_key,
            title=definition_title,
            statement=statement_without_columns,  # type: ignore [arg-type]
        )
        # Without defining columns, accessing headings should raise an error.
        with pytest.raises(
            PalaceValueError,
            match=rf"Unsupported statement in '{definition_title}' query \(id='{definition_key}'\).",
        ):
            _ = no_columns.headings

        # So, we add the columns explicitly.
        statement_with_columns = statement_without_columns.columns(
            id=Integer, name=String
        )

        definition = TabularQueryDefinition(
            key=definition_key,
            title=definition_title,
            statement=statement_with_columns,
        )

        # We can get the headings without executing the query.
        assert definition.headings == ("id", "name")

        # Request the rows using the bound parameter.
        rows1 = list(
            definition.rows(
                session=db.session, collection_name="Default Inactive Collection"
            )
        )
        rows2 = list(
            definition.rows(session=db.session, collection_name="Default Collection")
        )

        # Verify the correct rows are returned.
        assert rows1 == [(inactive.id, inactive.integration_configuration.name)]
        assert rows2 == [(active.id, active.integration_configuration.name)]
