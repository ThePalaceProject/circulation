from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.feed.annotator.base import Annotator
from palace.manager.feed.types import Author, WorkEntry
from palace.manager.metadata_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.work import Work


class VerboseAnnotator(Annotator):
    """The default Annotator for machine-to-machine integration.

    This Annotator describes all categories and authors for the book
    in great detail.
    """

    def annotate_work_entry(
        self, entry: WorkEntry, updated: datetime | None = None
    ) -> None:
        super().annotate_work_entry(entry, updated=updated)
        self.add_ratings(entry)

    @classmethod
    def add_ratings(cls, entry: WorkEntry) -> None:
        """Add a quality rating to the work."""
        work = entry.work
        for type_uri, value in [
            (Measurement.QUALITY, work.quality),
            (None, work.rating),
            (Measurement.POPULARITY, work.popularity),
        ]:
            if value and entry.computed:
                entry.computed.ratings.append(cls.rating(type_uri, value))

    @classmethod
    def categories(
        cls, work: Work, policy: PresentationCalculationPolicy | None = None
    ) -> dict[str, list[dict[str, str]]]:
        """Send out _all_ categories for the work.

        (So long as the category type has a URI associated with it in
        Subject.uri_lookup.)

        :param policy: A PresentationCalculationPolicy to
            use when deciding how deep to go when finding equivalent
            identifiers for the work.
        """
        policy = policy or PresentationCalculationPolicy(
            equivalent_identifier_cutoff=100
        )
        _db = Session.object_session(work)
        by_scheme_and_term = dict()
        identifier_ids = work.all_identifier_ids(policy=policy)
        classifications = Identifier.classifications_for_identifier_ids(
            _db, identifier_ids
        )
        for c in classifications:
            subject = c.subject
            if subject and subject.type in Subject.uri_lookup:
                scheme = Subject.uri_lookup[subject.type]
                term = subject.identifier
                weight_field = "ratingValue"
                key = (scheme, term)
                if not key in by_scheme_and_term:
                    value: dict[str, Any] = dict(term=subject.identifier)
                    if subject.name:
                        value["label"] = subject.name
                    value[weight_field] = 0
                    by_scheme_and_term[key] = value
                by_scheme_and_term[key][weight_field] += c.weight

        # Collapse by_scheme_and_term to by_scheme
        by_scheme = defaultdict(list)
        for (scheme, term), value in list(by_scheme_and_term.items()):
            by_scheme[scheme].append(value)
        by_scheme.update(super().categories(work))
        return by_scheme

    @classmethod
    def authors(cls, edition: Edition) -> dict[str, list[Author]]:
        """Create a detailed <author> tag for each author."""
        return {
            "authors": [
                cls.detailed_author(author) for author in edition.author_contributors
            ],
            "contributors": [],
        }

    @classmethod
    def detailed_author(cls, contributor: Contributor) -> Author:
        """Turn a Contributor into a detailed <author> tag."""
        author = Author()
        author.name = contributor.display_name
        author.sort_name = contributor.sort_name
        author.family_name = contributor.family_name
        author.wikipedia_name = contributor.wikipedia_name
        author.viaf = f"http://viaf.org/viaf/{contributor.viaf}"
        author.lc = f"http://id.loc.gov/authorities/names/{contributor.lc}"

        return author
