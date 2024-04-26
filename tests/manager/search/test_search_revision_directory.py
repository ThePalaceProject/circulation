from unittest import mock

import pytest

from palace.manager.search.document import SearchMappingDocument
from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.revision_directory import SearchRevisionDirectory


class AnyNumberRevision(SearchSchemaRevision):
    def __init__(self, number):
        self.SEARCH_VERSION = number
        super().__init__()

    def mapping_document(self) -> SearchMappingDocument:
        return SearchMappingDocument()


class TestSearchRevisionDirectory:
    def test_create(self):
        """Also tests _create_revisions"""
        with mock.patch("palace.manager.search.revision_directory.REVISIONS", new=[]):
            assert SearchRevisionDirectory.create().available == {}

        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[AnyNumberRevision(1), AnyNumberRevision(2)],
        ):
            assert list(SearchRevisionDirectory.create().available.keys()) == [1, 2]

        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[AnyNumberRevision(1), AnyNumberRevision(1)],
        ):
            with pytest.raises(ValueError) as raised:
                SearchRevisionDirectory.create()
            assert str(raised.value) == "Revision version 1 is defined multiple times"

    def test_highest(self):
        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[AnyNumberRevision(1), AnyNumberRevision(2)],
        ):
            assert SearchRevisionDirectory.create().highest().version == 2

        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[AnyNumberRevision(17), AnyNumberRevision(2)],
        ):
            assert SearchRevisionDirectory.create().highest().version == 17
