import json

import pytest

from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.search.pagination import Pagination, SortKeyPagination
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture


class TestSortKeyPagination:
    """Test the Opensearch-implementation of Pagination that does
    pagination by tracking the last item on the previous page,
    rather than by tracking the number of items seen so far.
    """

    def test_from_request(self):
        # No arguments -> Class defaults.
        pagination = SortKeyPagination.from_request({}.get, None)
        assert isinstance(pagination, SortKeyPagination)
        assert SortKeyPagination.DEFAULT_SIZE == pagination.size
        assert None == pagination.pagination_key

        # Override the default page size.
        pagination = SortKeyPagination.from_request({}.get, 100)
        assert isinstance(pagination, SortKeyPagination)
        assert 100 == pagination.size
        assert None == pagination.pagination_key

        # The most common usages.
        pagination = SortKeyPagination.from_request(dict(size="4").get)
        assert isinstance(pagination, SortKeyPagination)
        assert 4 == pagination.size
        assert None == pagination.pagination_key

        pagination_key = json.dumps(["field 1", 2])

        pagination = SortKeyPagination.from_request(dict(key=pagination_key).get)
        assert isinstance(pagination, SortKeyPagination)
        assert SortKeyPagination.DEFAULT_SIZE == pagination.size
        assert pagination_key == pagination.pagination_key

        # Invalid size -> problem detail
        error = SortKeyPagination.from_request(dict(size="string").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid page size: string" == str(error.detail)

        # Invalid pagination key -> problem detail
        error = SortKeyPagination.from_request(dict(key="not json").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid page key: not json" == str(error.detail)

        # Size too large -> cut down to MAX_SIZE
        pagination = SortKeyPagination.from_request(dict(size="10000").get)
        assert isinstance(pagination, SortKeyPagination)
        assert SortKeyPagination.MAX_SIZE == pagination.size
        assert None == pagination.pagination_key

    def test_items(self):
        # Test the values added to URLs to propagate pagination
        # settings across requests.
        pagination = SortKeyPagination(size=20)
        assert [("size", 20)] == list(pagination.items())
        key = ["the last", "item"]
        pagination.last_item_on_previous_page = key
        assert [("key", json.dumps(key)), ("size", 20)] == list(pagination.items())

    def test_pagination_key(self):
        # SortKeyPagination has no pagination key until it knows
        # about the last item on the previous page.
        pagination = SortKeyPagination()
        assert None == pagination.pagination_key

        key = ["the last", "item"]
        pagination.last_item_on_previous_page = key
        assert pagination.pagination_key == json.dumps(key)

    def test_unimplemented_features(self):
        # Check certain features of a normal Pagination object that
        # are not implemented in SortKeyPagination.

        # Set up a realistic SortKeyPagination -- certain things
        # will remain undefined.
        pagination = SortKeyPagination(last_item_on_previous_page=object())
        pagination.this_page_size = 100
        pagination.last_item_on_this_page = object()

        # The offset is always zero.
        assert 0 == pagination.offset

        # The total size is always undefined, even though we could
        # theoretically track it.
        assert None == pagination.total_size

        # The previous page is always undefined, through theoretically
        # we could navigate backwards.
        assert None == pagination.previous_page

        with pytest.raises(NotImplementedError) as excinfo:
            pagination.modify_database_query(object())
        assert "SortKeyPagination does not work with database queries." in str(
            excinfo.value
        )

    def test_modify_search_query(self):
        class MockSearch:
            update_from_dict_called_with = "not called"
            getitem_called_with = "not called"

            def update_from_dict(self, dict):
                self.update_from_dict_called_with = dict
                return self

            def __getitem__(self, slice):
                self.getitem_called_with = slice
                return "modified search object"

        search = MockSearch()

        # We start off in a state where we don't know the last item on the
        # previous page.
        pagination = SortKeyPagination()

        # In this case, modify_search_query slices out the first
        # 'page' of results and returns a modified search object.
        assert "modified search object" == pagination.modify_search_query(search)
        assert slice(0, 50) == search.getitem_called_with

        # update_from_dict was not called. We don't know where to
        # start our search, so we start at the beginning.
        assert "not called" == search.update_from_dict_called_with

        # Now let's say we find out the last item on the previous page
        # -- in real life, this would be because we call page_loaded()
        # and then next_page().
        last_item = object()
        pagination.last_item_on_previous_page = last_item

        # Reset the object so we can verify __getitem__ gets called
        # again.
        search.getitem_called_with = "not called"

        # With .last_item_on_previous_page set, modify_search_query()
        # calls update_from_dict() on our mock OpenSearch `Search`
        # object, passing in the last item on the previous page.

        # The return value of modify_search_query() becomes the active
        # Search object.
        assert "modified search object" == pagination.modify_search_query(search)

        # Now we can see that the Opensearch object was modified to
        # use the 'search_after' feature.
        assert dict(search_after=last_item) == search.update_from_dict_called_with

        # And the resulting object was modified _again_ to get the
        # first 'page' of results following last_item.
        assert slice(0, 50) == search.getitem_called_with

    def test_page_loaded(self):
        # Test what happens to a SortKeyPagination object when a page of
        # results is loaded.
        this_page = SortKeyPagination()

        # Mock an Opensearch 'hit' object -- we'll be accessing
        # hit.meta.sort.
        class MockMeta:
            def __init__(self, sort_key):
                self.sort = sort_key

        class MockItem:
            def __init__(self, sort_key):
                self.meta = MockMeta(sort_key)

        # Make a page of results, each with a unique sort key.
        hits = [MockItem(["sort", "key", num]) for num in range(5)]
        last_hit = hits[-1]

        # Tell the page about the results.
        assert False == this_page.page_has_loaded
        this_page.page_loaded(hits)
        assert True == this_page.page_has_loaded

        # We know the size.
        assert 5 == this_page.this_page_size

        # We know the sort key of the last item in the page.
        assert last_hit.meta.sort == this_page.last_item_on_this_page

        # This code has coverage elsewhere, but just so you see how it
        # works -- we can now get the next page...
        next_page = this_page.next_page

        # And it's defined in terms of the last item on its
        # predecessor. When we pass the new pagination object into
        # create_search_doc, it'll call this object's
        # modify_search_query method. The resulting search query will
        # pick up right where the previous page left off.
        assert last_hit.meta.sort == next_page.last_item_on_previous_page

    def test_next_page(self):
        # To start off, we can't say anything about the next page,
        # because we don't know anything about _this_ page.
        first_page = SortKeyPagination()
        assert None == first_page.next_page

        # Let's learn about this page.
        first_page.this_page_size = 10
        last_item = object()
        first_page.last_item_on_this_page = last_item

        # When we call next_page, the last item on this page becomes the
        # next page's "last item on previous_page"
        next_page = first_page.next_page
        assert last_item == next_page.last_item_on_previous_page

        # Again, we know nothing about this page, since we haven't
        # loaded it yet.
        assert None == next_page.this_page_size
        assert None == next_page.last_item_on_this_page

        # In the unlikely event that we know the last item on the
        # page, but the page size is zero, there is no next page.
        first_page.this_page_size = 0
        assert None == first_page.next_page


class TestPagination:
    def test_from_request(self):
        # No arguments -> Class defaults.
        pagination = Pagination.from_request({}.get, None)
        assert isinstance(pagination, Pagination)
        assert Pagination.DEFAULT_SIZE == pagination.size
        assert 0 == pagination.offset

        # Override the default page size.
        pagination = Pagination.from_request({}.get, 100)
        assert isinstance(pagination, Pagination)
        assert 100 == pagination.size
        assert 0 == pagination.offset

        # The most common usages.
        pagination = Pagination.from_request(dict(size="4").get)
        assert isinstance(pagination, Pagination)
        assert 4 == pagination.size
        assert 0 == pagination.offset

        pagination = Pagination.from_request(dict(after="6").get)
        assert isinstance(pagination, Pagination)
        assert Pagination.DEFAULT_SIZE == pagination.size
        assert 6 == pagination.offset

        pagination = Pagination.from_request(dict(size=4, after=6).get)
        assert isinstance(pagination, Pagination)
        assert 4 == pagination.size
        assert 6 == pagination.offset

        # Invalid size or offset -> problem detail
        error = Pagination.from_request(dict(size="string").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid page size: string" == str(error.detail)

        error = Pagination.from_request(dict(after="string").get)
        assert INVALID_INPUT.uri == error.uri
        assert "Invalid offset: string" == str(error.detail)

        # Size too large -> cut down to MAX_SIZE
        pagination = Pagination.from_request(dict(size="10000").get)
        assert isinstance(pagination, Pagination)
        assert Pagination.MAX_SIZE == pagination.size
        assert 0 == pagination.offset

    def test_has_next_page_total_size(self, db: DatabaseTransactionFixture):
        """Test the ability of Pagination.total_size to control whether there is a next page."""
        query = db.session.query(Work)
        pagination = Pagination(size=2)

        # When total_size is not set, Pagination assumes there is a
        # next page.
        pagination.modify_database_query(db.session, query)
        assert True == pagination.has_next_page

        # Here, there is one more item on the next page.
        pagination.total_size = 3
        assert 0 == pagination.offset
        assert True == pagination.has_next_page

        # Here, the last item on this page is the last item in the dataset.
        pagination.offset = 1
        assert False == pagination.has_next_page
        assert None == pagination.next_page

        # If we somehow go over the end of the dataset, there is no next page.
        pagination.offset = 400
        assert False == pagination.has_next_page
        assert None == pagination.next_page

        # If both total_size and this_page_size are set, total_size
        # takes precedence.
        pagination.offset = 0
        pagination.total_size = 100
        pagination.this_page_size = 0
        assert True == pagination.has_next_page

        pagination.total_size = 0
        pagination.this_page_size = 10
        assert False == pagination.has_next_page
        assert None == pagination.next_page

    def test_has_next_page_this_page_size(self, db: DatabaseTransactionFixture):
        """Test the ability of Pagination.this_page_size to control whether there is a next page."""
        query = db.session.query(Work)
        pagination = Pagination(size=2)

        # When this_page_size is not set, Pagination assumes there is a
        # next page.
        pagination.modify_database_query(db.session, query)
        assert True == pagination.has_next_page

        # Here, there is nothing on the current page. There is no next page.
        pagination.this_page_size = 0
        assert False == pagination.has_next_page

        # If the page is full, we can be almost certain there is a next page.
        pagination.this_page_size = 400
        assert True == pagination.has_next_page

        # Here, there is one item on the current page. Even though the
        # current page is not full (page size is 2), we assume for
        # safety's sake that there is a next page. The cost of getting
        # this wrong is low, compared to the cost of saying there is no
        # next page when there actually is.
        pagination.this_page_size = 1
        assert True == pagination.has_next_page

    def test_page_loaded(self):
        # Test page_loaded(), which lets the Pagination object see the
        # size of the current page.
        pagination = Pagination()
        assert None == pagination.this_page_size
        assert False == pagination.page_has_loaded
        pagination.page_loaded([1, 2, 3])
        assert 3 == pagination.this_page_size
        assert True == pagination.page_has_loaded

    def test_modify_search_query(self):
        # The default implementation of modify_search_query is to slice
        # a set of search results like a list.
        pagination = Pagination(offset=2, size=3)
        o = [1, 2, 3, 4, 5, 6]
        assert o[2 : 2 + 3] == pagination.modify_search_query(o)
