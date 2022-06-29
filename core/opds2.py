from typing import Dict, List

from core.external_search import ExternalSearchIndex
from core.lane import SearchFacets, WorkList
from core.model.collection import Collection
from core.model.contributor import Contribution, Contributor
from core.model.edition import Edition
from core.model.work import Work


class OPDS2Feed:
    pass


class FeedTypes:
    PUBLICATIONS = "publications"


class AcquisitonFeedOPDS2(OPDS2Feed):
    @classmethod
    def publications(
        cls,
        _db,
        worklist: WorkList,
        facets: SearchFacets,
        search_engine: ExternalSearchIndex,
    ):
        # do some caching magic
        # then do the publication

        return cls._generate_publications(_db, worklist, facets, search_engine)

    @classmethod
    def _generate_publications(
        cls,
        _db,
        worklist: WorkList,
        facets: SearchFacets,
        search_engine: ExternalSearchIndex,
    ):
        publications = []

        for work in worklist.works(_db, facets=facets, search_engine=search_engine):
            publications.append(work)

        return cls(_db, "publications", publications, worklist.get_library(_db))

    def __init__(
        self, _db, title, works: List[Work], library, feed_type=FeedTypes.PUBLICATIONS
    ):
        self._db = _db
        self.works = works
        self.library = library
        self.feed_type = feed_type

    def json(self):
        if self.feed_type == FeedTypes.PUBLICATIONS:
            return self.publications_json()

    def publications_json(self):
        result = {}

        entries = []
        for work in self.works:
            entries.append(self._metadata_for_work(work))

        result["publications"] = entries
        return result

    # Should this be in an annotator??
    def _metadata_for_work(self, work: Work):
        """Create the metadata json for a work item
        using the schema https://readium.org/webpub-manifest/context.jsonld"""
        # TODO: What happens when there is not presentation edition?
        edition: Edition = work.presentation_edition
        result = {}
        # Palace marketplace has this as '@type'
        result["type"] = Edition.medium_to_additional_type.get(edition.medium)
        result["title"] = edition.title
        result["subtitle"] = edition.subtitle
        result["identifier"] = edition.primary_identifier.identifier
        result["sortAs"] = edition.sort_title
        result.update(self._contributors(edition))
        result["language"] = edition.language
        # TODO: subject is meant to be http://schema.org/about,
        # however Palace marketplace uses this to provide genre subjects
        # TODO: numberOfPages. we don't store this
        # TODO: duration. we don't store this
        # TODO: abridged. we don't store this
        if edition.publisher:
            result["publisher"] = {"name": edition.publisher}
        if edition.imprint:
            result["imprint"] = {"name": edition.imprint}
        result["modified"] = work.last_update_time
        result["description"] = work.summary_text
        # TODO: belongsTo. Palace marketplace has series and collection within this,
        # which shouldn't be the case because it is a https://schema.org/CreativeWork
        # Even the OPDS example uses it in this way https://drafts.opds.io/opds-2.0.html#42-metadata
        if work.series:
            result["series"] = {
                "name": work.series,
            }
            # Palace marketplace has this within the "series" object
            result["position"] = (
                work.series_position if work.series_position is not None else 1
            )
        collection = self._collection(edition)
        if collection:
            result["collection"] = collection
        # TODO: add acquisition links

        return dict(metadata=result)

    def _collection(self, edition: Edition) -> Dict:
        """The first collection of this edition that is part of the library of this feed"""
        collection = None
        collection_ids = [c.id for c in self.library.all_collections]
        this_collection: Collection = None
        for pool in edition.license_pools:
            if pool.collection_id in collection_ids:
                this_collection = pool.collection
                break
        if this_collection:
            collection = {"name": this_collection.name}
        return collection

    def _contributors(self, edition: Edition) -> Dict:
        authors = {}
        contribution: Contribution
        key_mapping = {
            Contributor.AUTHOR_ROLE: "author",
            Contributor.TRANSLATOR_ROLE: "translator",
            Contributor.EDITOR_ROLE: "editor",
            Contributor.ILLUSTRATOR_ROLE: "illustrator",
            Contributor.ARTIST_ROLE: "artist",
            Contributor.COLORIST_ROLE: "colorist",
            Contributor.INKER_ROLE: "inker",
            Contributor.PENCILER_ROLE: "pencilor",
            Contributor.LETTERER_ROLE: "letterer",
            Contributor.NARRATOR_ROLE: "narrator",
            Contributor.CONTRIBUTOR_ROLE: "contributor",
        }
        for contribution in edition.contributions:
            if contribution.role in key_mapping:
                contributor = contribution.contributor
                meta = {"name": contributor.display_name}
                if len(contributor.aliases) > 0:
                    meta["additionalName"] = contributor.aliases[0]

                # TODO: Marketplace adds links for the author based search
                # should we do the same?

                authors[key_mapping[contribution.role]] = meta
        return authors

    def __str__(self):
        """Make the serialized OPDS2 feed"""
