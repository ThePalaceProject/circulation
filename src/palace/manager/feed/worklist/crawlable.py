from palace.manager.feed.worklist.dynamic import DynamicLane
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.library import Library


class CrawlableLane(DynamicLane):
    # By default, crawlable feeds are cached for 12 hours.
    MAX_CACHE_AGE = 12 * 60 * 60


class CrawlableCollectionBasedLane(CrawlableLane):
    # Since these collections may be shared collections, for which
    # recent information is very important, these feeds are only
    # cached for 5 minutes.
    MAX_CACHE_AGE = 5 * 60

    LIBRARY_ROUTE = "crawlable_library_feed"
    COLLECTION_ROUTE = "crawlable_collection_feed"

    def initialize(self, library_or_collections: Library | list[Collection]):  # type: ignore[override]
        self.collection_feed = False

        if isinstance(library_or_collections, Library):
            # We're looking at only the active collections for the given library.
            library = library_or_collections
            collections = library.active_collections
            identifier = library.name
        elif isinstance(library_or_collections, list):
            # We're looking at collections directly, without respect
            # to the libraries that might use them.
            library = None
            collections = library_or_collections
            identifier = " / ".join(sorted(x.name for x in collections))
            if len(collections) == 1:
                self.collection_feed = True
                self.collection_name = collections[0].name

        super().initialize(
            library,
            "Crawlable feed: %s" % identifier,
        )
        if collections is not None:
            # initialize() set the collection IDs to all collections
            # associated with the library. We may want to restrict that
            # further.
            self.collection_ids = [x.id for x in collections]

    @property
    def url_arguments(self):
        if not self.collection_feed:
            return self.LIBRARY_ROUTE, dict()
        else:
            kwargs = dict(
                collection_name=self.collection_name,
            )
            return self.COLLECTION_ROUTE, kwargs


class CrawlableCustomListBasedLane(CrawlableLane):
    """A lane that consists of all works in a single CustomList."""

    ROUTE = "crawlable_list_feed"

    uses_customlists = True

    def initialize(self, library, customlist):
        self.customlist_name = customlist.name
        super().initialize(
            library,
            "Crawlable feed: %s" % self.customlist_name,
            customlists=[customlist],
        )

    @property
    def url_arguments(self):
        kwargs = dict(list_name=self.customlist_name)
        return self.ROUTE, kwargs
