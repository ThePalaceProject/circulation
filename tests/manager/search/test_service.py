from unittest.mock import MagicMock

import pytest

from palace.manager.search.document import LONG
from palace.manager.search.service import (
    SearchDocument,
    SearchPointer,
    SearchServiceOpensearch1,
)
from tests.fixtures.search import ExternalSearchFixture
from tests.mocks.search import MockSearchSchemaRevision


class TestSearchPointer:
    @pytest.mark.parametrize(
        "base, index, expected_version",
        [
            ("base", "base-v23", 23),
            ("base", "base-v42", 42),
            ("base", "base-v0", 0),
            ("base", "base-v1", 1),
            ("base", "base-v99", 99),
        ],
    )
    def test_from_index(self, base: str, index: str, expected_version: int):
        service = SearchServiceOpensearch1(MagicMock(), base)

        write_pointer = SearchPointer.from_index(
            base, service.write_pointer_name(), index
        )
        assert write_pointer is not None
        assert write_pointer.index == index
        assert write_pointer.version == expected_version
        assert write_pointer.alias == service.write_pointer_name()

        read_pointer = SearchPointer.from_index(
            base, service.read_pointer_name(), index
        )
        assert read_pointer is not None
        assert read_pointer.index == index
        assert read_pointer.version == expected_version
        assert read_pointer.alias == service.read_pointer_name()

    @pytest.mark.parametrize(
        "base, index",
        [
            ("base", "nbase-v23"),
            ("base", "base-42"),
            ("base", "basee-42"),
            ("base", "base"),
            ("base", "basev1"),
            ("base", "base-v99abc"),
        ],
    )
    def test_from_index_errors(self, base: str, index: str):
        service = SearchServiceOpensearch1(MagicMock(), base)

        assert (
            SearchPointer.from_index(base, service.write_pointer_name(), index) is None
        )
        assert (
            SearchPointer.from_index(base, service.read_pointer_name(), index) is None
        )


class TestService:
    """
    Tests to verify that the Opensearch service implementation has the semantics we expect.
    """

    def test_create_index_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Creating any index is idempotent."""
        service = external_search_fixture.service
        revision = MockSearchSchemaRevision(23)
        service.index_create(revision)
        service.index_create(revision)

        indices = external_search_fixture.client.indices.client.indices
        assert indices is not None
        assert indices.exists(
            index=revision.name_for_index(external_search_fixture.index_prefix)
        )

    def test_read_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The read pointer is initially unset."""
        service = external_search_fixture.service
        assert service.read_pointer() is None

    def test_write_pointer_none(self, external_search_fixture: ExternalSearchFixture):
        """The write pointer is initially unset."""
        service = external_search_fixture.service
        assert service.write_pointer() is None

    def test_read_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the read pointer works."""
        service = external_search_fixture.service
        revision = MockSearchSchemaRevision(23)
        service.index_create(revision)

        service.read_pointer_set(revision)
        pointer = service.read_pointer()
        assert pointer is not None
        assert pointer.index == f"{external_search_fixture.index_prefix}-v23"

    def test_write_pointer_set(self, external_search_fixture: ExternalSearchFixture):
        """Setting the write pointer works."""
        service = external_search_fixture.service
        revision = MockSearchSchemaRevision(23)
        service.index_create(revision)

        service.write_pointer_set(revision)

        pointer = service.write_pointer()
        assert pointer is not None
        assert pointer.index == f"{external_search_fixture.index_prefix}-v23"

    def test_populate_index_idempotent(
        self, external_search_fixture: ExternalSearchFixture
    ):
        """Populating an index is idempotent."""
        service = external_search_fixture.service
        revision = MockSearchSchemaRevision(23)

        mappings = revision.mapping_document()
        mappings.properties["x"] = LONG
        mappings.properties["y"] = LONG

        # The format expected by the opensearch bulk helper is completely undocumented.
        # It does, however, appear to use mostly the same format as the Elasticsearch equivalent.
        # See: https://elasticsearch-py.readthedocs.io/en/v7.13.1/helpers.html#bulk-helpers
        documents: list[SearchDocument] = [
            {
                "_index": revision.name_for_index("base"),
                "_type": "_doc",
                "_id": 1,
                "_source": {"x": 23, "y": 24},
            },
            {
                "_index": revision.name_for_index("base"),
                "_type": "_doc",
                "_id": 2,
                "_source": {"x": 25, "y": 26},
            },
            {
                "_index": revision.name_for_index("base"),
                "_type": "_doc",
                "_id": 3,
                "_source": {"x": 27, "y": 28},
            },
        ]

        service.index_create(revision)

        service.index_submit_documents(documents)
        service.index_submit_documents(documents)

        indices = external_search_fixture.client.indices.client.indices
        assert indices is not None
        assert indices.exists(
            index=revision.name_for_index(external_search_fixture.index_prefix)
        )
        assert indices.get(
            index=revision.name_for_index(external_search_fixture.index_prefix)
        )[f"{external_search_fixture.index_prefix}-v23"]["mappings"] == {
            "properties": mappings.serialize_properties()
        }

    def test__get_pointer(self):
        """Getting a pointer works."""
        mock_client = MagicMock()
        mock_client.indices.get_alias.return_value = {
            "base-v23": {"aliases": {"base-search-read": {}}}
        }
        service = SearchServiceOpensearch1(mock_client, "base")

        pointer = service._get_pointer("base-search-read")
        assert pointer is not None
        assert pointer.index == "base-v23"
        assert pointer.version == 23
        mock_client.indices.get_alias.assert_called_once_with(name="base-search-read")

        mock_client.indices.get_alias.reset_mock()
        mock_client.indices.get_alias.return_value = {"bad": [], "data": []}
        pointer = service._get_pointer("base-search-read")
        assert pointer is None
        mock_client.indices.get_alias.assert_called_once_with(name="base-search-read")
