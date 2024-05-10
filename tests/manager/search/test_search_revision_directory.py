from unittest import mock

import pytest

from palace.manager.search.revision_directory import SearchRevisionDirectory
from tests.mocks.search import MockSearchSchemaRevision


class TestSearchRevisionDirectory:
    def test_create(self):
        """Also tests _create_revisions"""
        with mock.patch("palace.manager.search.revision_directory.REVISIONS", new=[]):
            assert SearchRevisionDirectory.create().available == {}

        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[MockSearchSchemaRevision(1), MockSearchSchemaRevision(2)],
        ):
            assert list(SearchRevisionDirectory.create().available.keys()) == [1, 2]

        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[MockSearchSchemaRevision(1), MockSearchSchemaRevision(1)],
        ):
            with pytest.raises(ValueError) as raised:
                SearchRevisionDirectory.create()
            assert str(raised.value) == "Revision version 1 is defined multiple times"

    def test_highest(self):
        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[MockSearchSchemaRevision(1), MockSearchSchemaRevision(2)],
        ):
            assert SearchRevisionDirectory.create().highest().version == 2

        with mock.patch(
            "palace.manager.search.revision_directory.REVISIONS",
            new=[MockSearchSchemaRevision(17), MockSearchSchemaRevision(2)],
        ):
            assert SearchRevisionDirectory.create().highest().version == 17
