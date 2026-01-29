from palace.manager.feed.worklist.base import WorkList
from palace.manager.sqlalchemy.model.edition import Edition


class KnownOverviewFacetsWorkList(WorkList):
    """A WorkList whose defining feature is that the Facets object
    to be used when generating a grouped feed is known in advance.
    """

    def __init__(self, facets, *args, **kwargs):
        """Constructor.

        :param facets: A Facets object to be used when generating a grouped
           feed.
        """
        super().__init__(*args, **kwargs)
        self.facets = facets

    def overview_facets(self, _db, facets):
        """Return the faceting object to be used when generating a grouped
        feed.

        :param _db: Ignored -- only present for API compatibility.
        :param facets: Ignored -- only present for API compatibility.
        """
        return self.facets


class JackpotWorkList(WorkList):
    """A WorkList guaranteed to, so far as possible, contain the exact
    selection of books necessary to perform common QA tasks.

    This makes it easy to write integration tests that work on real
    circulation managers and real books.
    """

    def __init__(self, library, facets):
        """Constructor.

        :param library: A Library
        :param facets: A Facets object.
        """
        super().initialize(library)

        # Initialize a list of child Worklists; one for each test that
        # a client might need to run.
        self.children = []

        # Add one or more WorkLists for every active collection for the
        # library, so that a client can test borrowing a book from
        # any of them.
        for collection in sorted(library.active_collections, key=lambda x: x.name):
            for medium in Edition.FULFILLABLE_MEDIA:
                # Give each Worklist a name that is distinctive
                # and easy for a client to parse.
                if collection.data_source:
                    data_source_name = collection.data_source.name
                else:
                    data_source_name = "[Unknown]"
                display_name = (
                    "License source {%s} - Medium {%s} - Collection name {%s}"
                    % (data_source_name, medium, collection.name)
                )
                child = KnownOverviewFacetsWorkList(facets)
                child.initialize(library, media=[medium], display_name=display_name)
                child.collection_ids = [collection.id]
                self.children.append(child)

    def works(self, _db, *args, **kwargs):
        """This worklist never has works of its own.

        Only its children have works.
        """
        return []
