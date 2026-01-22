from __future__ import annotations

import json

from flask_babel import lazy_gettext as _

from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.util.problem_detail import ProblemDetail


class Pagination:
    DEFAULT_SIZE = 50
    DEFAULT_SEARCH_SIZE = 10
    DEFAULT_FEATURED_SIZE = 10
    DEFAULT_CRAWLABLE_SIZE = 100
    MAX_SIZE = 100

    @classmethod
    def default(cls):
        return Pagination(0, cls.DEFAULT_SIZE)

    def __init__(self, offset=0, size=DEFAULT_SIZE):
        """Constructor.

        :param offset: Start pulling entries from the query at this index.
        :param size: Pull no more than this number of entries from the query.
        """
        self.offset = offset
        self.size = size
        self.total_size = None
        self.this_page_size = None
        self.page_has_loaded = False
        self.max_size = self.MAX_SIZE

    @classmethod
    def _int_from_request(cls, key, get_arg, make_detail, default):
        """Helper method to get and parse an integer value from
        a URL query argument in a Flask request.

        :param key: Name of the argument.
        :param get_arg: A function which when called with (key, default)
           returns the value of the query argument.
        :pass make_detail: A function, called with the value
           obtained from the request, which returns the detail
           information that should be included in a problem detail
           document if the input isn't convertable to an integer.
        :param default: Use this value if none is specified.
        """
        raw = get_arg(key, default)
        try:
            as_int = int(raw)
        except ValueError:
            return INVALID_INPUT.detailed(make_detail(raw))
        return as_int

    @classmethod
    def size_from_request(cls, get_arg, default):
        make_detail = lambda size: (_("Invalid page size: %(size)s", size=size))
        size = cls._int_from_request(
            "size", get_arg, make_detail, default or cls.DEFAULT_SIZE
        )
        if isinstance(size, ProblemDetail):
            return size
        return min(size, cls.MAX_SIZE)

    @classmethod
    def from_request(cls, get_arg, default_size=None):
        """Instantiate a Pagination object from a Flask request."""
        default_size = default_size or cls.DEFAULT_SIZE
        size = cls.size_from_request(get_arg, default_size)
        if isinstance(size, ProblemDetail):
            return size
        offset = cls._int_from_request(
            "after",
            get_arg,
            lambda offset: _("Invalid offset: %(offset)s", offset=offset),
            0,
        )
        if isinstance(offset, ProblemDetail):
            return offset
        return cls(offset, size)

    def items(self):
        yield ("after", self.offset)
        yield ("size", self.size)

    @property
    def query_string(self):
        return "&".join("=".join(map(str, x)) for x in list(self.items()))

    @property
    def first_page(self):
        return Pagination(0, self.size)

    @property
    def next_page(self):
        if not self.has_next_page:
            return None
        return Pagination(self.offset + self.size, self.size)

    @property
    def previous_page(self):
        if self.offset <= 0:
            return None
        previous_offset = self.offset - self.size
        previous_offset = max(0, previous_offset)
        return Pagination(previous_offset, self.size)

    @property
    def has_next_page(self):
        """Returns boolean reporting whether pagination is done for a query

        Either `total_size` or `this_page_size` must be set for this
        method to be accurate.
        """
        if self.total_size is not None:
            # We know the total size of the result set, so we know
            # whether or not there are more results.
            return self.offset + self.size < self.total_size
        if self.this_page_size is not None:
            # We know the number of items on the current page. If this
            # page was empty, we can assume there is no next page; if
            # not, we can assume there is a next page. This is a little
            # more conservative than checking whether we have a 'full'
            # page.
            return self.this_page_size > 0

        # We don't know anything about this result set, so assume there is
        # a next page.
        return True

    def modify_database_query(self, _db, qu):
        """Modify the given database query with OFFSET and LIMIT."""
        return qu.offset(self.offset).limit(self.size)

    def modify_search_query(self, search):
        """Modify a Search object so that it retrieves only a single 'page'
        of results.

        :return: A Search object.
        """
        return search[self.offset : self.offset + self.size]

    def page_loaded(self, page):
        """An actual page of results has been fetched. Keep any internal state
        that would be useful to know when reasoning about earlier or
        later pages.
        """
        self.this_page_size = len(page)
        self.page_has_loaded = True


class SortKeyPagination(Pagination):
    """An Opensearch-specific implementation of Pagination that
    paginates search results by tracking where in a sorted list the
    previous page left off, rather than using a numeric index into the
    list.
    """

    def __init__(self, last_item_on_previous_page=None, size=Pagination.DEFAULT_SIZE):
        self.size = size
        self.last_item_on_previous_page = last_item_on_previous_page

        # These variables are set by page_loaded(), after the query
        # is run.
        self.page_has_loaded = False
        self.last_item_on_this_page = None
        self.this_page_size = None

    @classmethod
    def from_request(cls, get_arg, default_size=None):
        """Instantiate a SortKeyPagination object from a Flask request."""
        size = cls.size_from_request(get_arg, default_size)
        if isinstance(size, ProblemDetail):
            return size
        pagination_key = get_arg("key", None)
        if pagination_key:
            try:
                pagination_key = json.loads(pagination_key)
            except ValueError as e:
                return INVALID_INPUT.detailed(
                    _("Invalid page key: %(key)s", key=pagination_key)
                )
        return cls(pagination_key, size)

    def items(self):
        """Yield the URL arguments necessary to convey the current page
        state.
        """
        pagination_key = self.pagination_key
        if pagination_key:
            yield ("key", self.pagination_key)
        yield ("size", self.size)

    @property
    def pagination_key(self):
        """Create the pagination key for this page."""
        if not self.last_item_on_previous_page:
            return None
        return json.dumps(self.last_item_on_previous_page)

    @property
    def offset(self):
        # This object never uses the traditional offset system; offset
        # is determined relative to the last item on the previous
        # page.
        return 0

    @property
    def total_size(self):
        # Although we technically know the total size after the first
        # page of results has been obtained, we don't use this feature
        # in pagination, so act like we don't.
        return None

    def modify_database_query(self, qu):
        raise NotImplementedError(
            "SortKeyPagination does not work with database queries."
        )

    def modify_search_query(self, search):
        """Modify the given Search object so that it starts
        picking up items immediately after the previous page.

        :param search: An opensearch-dsl Search object.
        """
        if self.last_item_on_previous_page:
            search = search.update_from_dict(
                dict(search_after=self.last_item_on_previous_page)
            )
        return super().modify_search_query(search)

    @property
    def previous_page(self):
        # TODO: We can get the previous page by flipping the sort
        # order and asking for the _next_ page of the reversed list,
        # using the sort keys of the _first_ item as the search_after.
        # But this is really confusing, it requires more context than
        # SortKeyPagination currently has, and this feature isn't
        # necessary for our current implementation.
        return None

    @property
    def next_page(self):
        """If possible, create a new SortKeyPagination representing the
        next page of results.
        """
        if self.this_page_size == 0:
            # This page is empty; there is no next page.
            return None
        if not self.last_item_on_this_page:
            # This probably means load_page wasn't called. At any
            # rate, we can't say anything about the next page.
            return None
        return SortKeyPagination(self.last_item_on_this_page, self.size)

    def page_loaded(self, page):
        """An actual page of results has been fetched. Keep any internal state
        that would be useful to know when reasoning about earlier or
        later pages.

        Specifically, keep track of the sort value of the last item on
        this page, so that self.next_page will create a
        SortKeyPagination object capable of generating the subsequent
        page.

        :param page: A list of opensearch-dsl Hit objects.
        """
        super().page_loaded(page)
        if page:
            last_item = page[-1]
            values = list(last_item.meta.sort)
        else:
            # There's nothing on this page, so there's no next page
            # either.
            values = None
        self.last_item_on_this_page = values
