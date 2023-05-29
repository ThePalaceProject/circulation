import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from core.external_search import ExternalSearchIndex
from core.lane import Facets, Pagination, WorkList
from core.model.cachedfeed import CachedFeed
from core.model.classification import Genre, Subject
from core.model.contributor import Contribution, Contributor
from core.model.edition import Edition
from core.model.licensing import LicensePool
from core.model.resource import Hyperlink
from core.model.work import Work


class OPDS2Feed:
    pass


class OPDS2Annotator:
    """Annotate a feed following the OPDS2 spec"""

    OPDS2_TYPE = "application/opds+json"

    def __init__(self, url, facets, pagination, library, title="OPDS2 Feed") -> None:
        self.url = url
        self.facets: Facets = facets
        self.library = library
        self.title = title
        self.pagination = pagination or Pagination()

    def metadata_for_work(self, work: Work) -> Optional[Dict[str, Any]]:
        """Create the metadata json for a work item
        using the schema https://drafts.opds.io/schema/publication.schema.json"""

        # TODO: What happens when there is no presentation edition?
        edition: Edition = work.presentation_edition
        if not edition:
            return None

        pool = self._pool_for_library(edition)
        result: Dict[str, Any] = {}
        result["@type"] = Edition.medium_to_additional_type.get(str(edition.medium))
        result["title"] = edition.title
        result["subtitle"] = edition.subtitle
        result["identifier"] = edition.primary_identifier.identifier
        result["sortAs"] = edition.sort_title
        result.update(self._contributors(edition))
        result["language"] = edition.language_code

        subjects = []
        genre: Genre
        for genre in work.genres:
            subjects.append(
                {
                    "scheme": Subject.SIMPLIFIED_GENRE,
                    "name": genre.name,
                    "sortAs": genre.name,
                }
            )
        if subjects:
            result["subject"] = subjects

        # TODO: numberOfPages. we don't store this
        # TODO: duration. we don't store this
        # TODO: abridged. we don't store this
        if edition.publisher:
            result["publisher"] = {"name": edition.publisher}
        if edition.imprint:
            result["imprint"] = {"name": edition.imprint}
        if work.last_update_time:
            result["modified"] = work.last_update_time.isoformat()
        if pool and pool.availability_time:
            result["published"] = pool.availability_time.isoformat()
        result["description"] = work.summary_text

        belongs_to = {}
        if work.series:
            belongs_to["series"] = {
                "name": work.series,
                "position": work.series_position
                if work.series_position is not None
                else 1,
            }

        if belongs_to:
            result["belongsTo"] = belongs_to

        # TODO: Collection, what does this stand for?

        links = self._work_metadata_links(edition)
        image_links = self.resource_links(
            edition, Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE, Hyperlink.ILLUSTRATION
        )

        return dict(metadata=result, links=links, images=image_links)

    def _work_metadata_links(self, edition: Edition):
        """Create links for works in the publication"""
        samples = self.resource_links(edition, Hyperlink.SAMPLE)
        open_access = self.resource_links(edition, Hyperlink.OPEN_ACCESS_DOWNLOAD)
        loan_link = self.loan_link(edition)
        self_link = self.self_link(edition)
        links = []
        if open_access:
            links.extend(open_access)
        if samples:
            links.extend(samples)
        if loan_link:
            links.append(loan_link)
        if self_link:
            links.append(self_link)
        return links

    def resource_links(self, edition: Edition, *rels) -> List[Dict]:
        """Create a link entry based on a stored Resource"""
        link: Hyperlink
        samples = []
        for link in edition.primary_identifier.links:
            if link.rel in rels:
                samples.append(
                    {
                        "href": link.resource.url,
                        "rel": link.rel,
                        "type": link.resource.representation.media_type,
                    }
                )
        return samples

    def loan_link(self, edition: Edition) -> Optional[Dict]:
        """Create a Loan link for an edition, needs access to the API layer
        Must be implemented in the API layer"""
        return None

    def self_link(self, edition: Edition) -> Optional[Dict]:
        """Create a Self link for an edition, needs access to the API layer
        Must be implemented in the API layer"""
        return None

    def _pool_for_library(self, edition: Edition) -> Optional[LicensePool]:
        """Fetch the licensepool of an edition that is part of the library we're annotating with"""
        collection_ids = [c.id for c in self.library.all_collections]
        for pool in edition.license_pools:
            if pool.collection_id in collection_ids:
                return pool
        return None

    def _contributors(self, edition: Edition) -> Dict:
        """Create the contributor type entries"""
        authors = {}
        contribution: Contribution
        key_mapping = {
            Contributor.PRIMARY_AUTHOR_ROLE: "author",
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
                if contributor.aliases and len(contributor.aliases) > 0:
                    meta["additionalName"] = contributor.aliases[0]

                # TODO: Marketplace adds links for the author based search
                # should we do the same?
                authors[key_mapping[contribution.role]] = meta
        return authors

    def feed_links(self):
        """Create links for a publication feed"""
        links = [
            {"href": self.url, "rel": "self", "type": self.OPDS2_TYPE},
        ]
        # If another page is present, then add the next link
        if self.pagination.has_next_page:
            next_query_string = urlencode(
                {
                    **dict(self.pagination.next_page.items()),
                    **dict(self.facets.items()),
                },
                doseq=True,
            )
            next_url = self.url.split("?", 1)[0] + "?" + next_query_string
            links.append({"href": next_url, "rel": "next", "type": self.OPDS2_TYPE})

        return links

    def feed_metadata(self):
        """Create the metadata for a publication feed"""
        return {
            "title": self.title,
            "itemsPerPage": self.pagination.size,
        }


class FeedTypes:
    """The types of feeds supported for OPDS2"""

    PUBLICATIONS = "publications"
    NAVIGATION = "navigation"


class AcquisitonFeedOPDS2(OPDS2Feed):
    """Creates different kinds of OPDS2 feeds
    Currently supports publications and navigation"""

    @classmethod
    def publications(
        cls,
        _db,
        worklist: WorkList,
        facets: Facets,
        pagination: Pagination,
        search_engine: ExternalSearchIndex,
        annotator: OPDS2Annotator,
        max_age: Optional[int] = None,
    ):
        """The publication feed, cached"""
        # do some caching magic
        # then do the publication
        def refresh():
            return cls._generate_publications(
                _db, worklist, facets, pagination, search_engine, annotator
            )

        return CachedFeed.fetch(
            _db,
            worklist=worklist,
            facets=facets,
            pagination=pagination,
            refresher_method=refresh,
            max_age=max_age,
        )

    @classmethod
    def _generate_publications(
        cls,
        _db,
        worklist: WorkList,
        facets: Facets,
        pagination: Pagination,
        search_engine: ExternalSearchIndex,
        annotator: OPDS2Annotator,
    ):
        publications = []

        for work in worklist.works(
            _db, facets=facets, search_engine=search_engine, pagination=pagination
        ):
            publications.append(work)

        return cls(
            _db,
            publications,
            annotator,
        )

    @classmethod
    def navigation(cls, _db, annotator: OPDS2Annotator):
        """The navigation feed"""
        return cls(_db, [], annotator, feed_type=FeedTypes.NAVIGATION)

    def __init__(
        self,
        _db,
        works: List[Work],
        annotator: OPDS2Annotator,
        feed_type=FeedTypes.PUBLICATIONS,
    ):
        self._db = _db
        self.works = works
        self.annotator = annotator
        self.feed_type = feed_type

    def json(self):
        """The a json feed based on the FeedType"""
        if self.feed_type == FeedTypes.PUBLICATIONS:
            return self.publications_json()
        elif self.feed_type == FeedTypes.NAVIGATION:
            return self.navigation_json()

    def navigation_json(self):
        return {
            "metadata": self.annotator.feed_metadata(),
            "links": self.annotator.feed_links(),
            "navigation": self.annotator.navigation_collection(),
        }

    def publications_json(self):
        result = {}

        entries = []
        for work in self.works:
            entry = self.annotator.metadata_for_work(work)
            if entry:
                entries.append(entry)

        result["publications"] = entries
        result["links"] = self.annotator.feed_links()
        result["metadata"] = self.annotator.feed_metadata()
        return result

    def __str__(self):
        """Make the serialized OPDS2 feed"""
        return json.dumps(self.json())
