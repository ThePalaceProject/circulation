from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy import bindparam, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

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

    def test_with_orm_query(self, db: DatabaseTransactionFixture):
        _l = db.default_library()
        _c = db.default_collection()
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
        # Request the rows using the bound parameter.
        rows = list(
            definition.rows(
                session=db.session, collection_name="Default Inactive Collection"
            )
        )

        assert definition.headings == ("id", "name")
        assert rows == [(inactive.id, inactive.integration_configuration.name)]
