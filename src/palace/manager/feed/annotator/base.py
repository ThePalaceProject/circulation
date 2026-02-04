from __future__ import annotations

import datetime
import logging
from collections import defaultdict
from decimal import Decimal
from functools import cmp_to_key
from typing import Any
from urllib.parse import quote

from sqlalchemy.orm import Session, joinedload

from palace.manager.core.classifier import Classifier
from palace.manager.feed.types import (
    Author,
    Category,
    Distribution,
    FeedData,
    Link,
    Rating,
    RichText,
    Series,
    WorkEntry,
    WorkEntryData,
)
from palace.manager.feed.util import strftime
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.opds_writer import AtomFeed, OPDSFeed


class ToFeedEntry:
    @classmethod
    def authors(cls, edition: Edition) -> dict[str, list[Author]]:
        """Create one or more author (and contributor) objects for the given
        Work.

        :param edition: The Edition to use as a reference
            for bibliographic information, including the list of
            Contributions.
        :return: A dict with "authors" and "contributors" as a list of Author objects
        """
        authors: dict[str, list[Author]] = {"authors": [], "contributors": []}
        state: dict[str | None, set[str]] = defaultdict(set)
        for contribution in edition.contributions:
            info = cls.contributor(contribution, state)
            if info is None:
                # contributor_tag decided that this contribution doesn't
                # need a tag.
                continue
            key, tag = info
            authors[f"{key}s"].append(tag)

        if authors["authors"]:
            return authors

        # We have no author information, so we add empty <author> tag
        # to avoid the implication (per RFC 4287 4.2.1) that this book
        # was written by whoever wrote the OPDS feed.
        authors["authors"].append(Author(name=""))
        return authors

    @classmethod
    def contributor(
        cls, contribution: Contribution, state: dict[str | None, set[str]]
    ) -> tuple[str, Author] | None:
        """Build an author (or contributor) object for a Contribution.

        :param contribution: A Contribution.
        :param state: A defaultdict of sets, which may be used to keep
            track of what happened during previous calls to
            contributor for a given Work.
        :return: An Author object, or None if creating an Author for this Contribution
            would be redundant or of low value.

        """
        contributor = contribution.contributor
        role = contribution.role
        current_role: str

        if role in Contributor.AUTHOR_ROLES:
            current_role = "author"
            marc_role = None
        else:
            current_role = "contributor"
            marc_role = Contributor.MARC_ROLE_CODES.get(role)
            if not marc_role:
                # This contribution is not one that we publish as
                # a <atom:contributor> tag. Skip it.
                return None

        name = contributor.display_name or contributor.sort_name
        name_key = name and name.lower()
        if not name_key or name_key in state[marc_role]:
            # Either there is no valid name present or
            # we've already credited this person with this
            # MARC role. Returning a tag would be redundant.
            return None

        # Okay, we're creating a tag.
        properties: dict[str, Any] = dict()
        if marc_role:
            properties["role"] = marc_role
        entry = Author(name=name, **properties)

        # Record the fact that we credited this person with this role,
        # so that we don't do it again on a subsequent call.
        state[marc_role].add(name_key)
        return current_role, entry

    @classmethod
    def series(
        cls, series_name: str | None, series_position: int | None | str | None
    ) -> Series | None:
        """Generate a Series object for the given name and position."""
        if not series_name:
            return None
        position = str(series_position) if series_position is not None else None
        return Series(name=series_name, position=position)

    @classmethod
    def rating(cls, type_uri: str | None, value: float | Decimal) -> Rating:
        """Generate a Rating object for the given type and value."""
        return Rating(rating_value="%.4f" % value, additional_type=type_uri)

    @classmethod
    def samples(cls, edition: Edition | None) -> list[Hyperlink]:
        if not edition:
            return []
        _db = Session.object_session(edition)
        links = (
            _db.query(Hyperlink)
            .filter(
                Hyperlink.rel == Hyperlink.SAMPLE,
                Hyperlink.identifier_id == edition.primary_identifier_id,
            )
            .options(joinedload(Hyperlink.resource))
            .all()
        )
        return links

    @classmethod
    def categories(cls, work: Work) -> dict[str, list[dict[str, str]]]:
        """Return all relevant classifications of this work.

        :return: A dictionary mapping 'scheme' URLs to dictionaries of
            attribute-value pairs.

        Notable attributes: 'term', 'label', 'ratingValue'
        """
        if not work:
            return {}

        categories = {}

        fiction_term = None
        if work.fiction == True:
            fiction_term = "Fiction"
        elif work.fiction == False:
            fiction_term = "Nonfiction"
        if fiction_term:
            fiction_scheme = Subject.SIMPLIFIED_FICTION_STATUS
            categories[fiction_scheme] = [
                dict(term=fiction_scheme + fiction_term, label=fiction_term)
            ]

        simplified_genres = []
        for wg in work.work_genres:
            simplified_genres.append(wg.genre.name)

        if simplified_genres:
            categories[Subject.SIMPLIFIED_GENRE] = [
                dict(term=Subject.SIMPLIFIED_GENRE + quote(x), label=x)
                for x in simplified_genres
                if x
            ]

        # Add the appeals as a category of schema
        # http://librarysimplified.org/terms/appeal
        schema_url = AtomFeed.SIMPLIFIED_NS + "appeals/"
        appeals: list[dict[str, Any]] = []
        categories[schema_url] = appeals
        for name, value in (
            (Work.CHARACTER_APPEAL, work.appeal_character),
            (Work.LANGUAGE_APPEAL, work.appeal_language),
            (Work.SETTING_APPEAL, work.appeal_setting),
            (Work.STORY_APPEAL, work.appeal_story),
        ):
            if value:
                appeal: dict[str, Any] = dict(term=schema_url + name, label=name)
                weight_field = "ratingValue"
                appeal[weight_field] = value
                appeals.append(appeal)

        # Add the audience as a category of schema
        # http://schema.org/audience
        if work.audience:
            audience_uri = "http://schema.org/audience"
            categories[audience_uri] = [dict(term=work.audience, label=work.audience)]

        # Any book can have a target age, but the target age
        # is only relevant for childrens' and YA books.
        audiences_with_target_age = (
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_YOUNG_ADULT,
        )
        if work.target_age and work.audience in audiences_with_target_age:
            uri = Subject.uri_lookup[Subject.AGE_RANGE]
            target_age = work.target_age_string
            if target_age:
                categories[uri] = [dict(term=target_age, label=target_age)]

        return categories

    @classmethod
    def content(cls, work: Work | None) -> str:
        """Return an HTML summary of this work."""
        summary = ""
        if work:
            if work.summary_text is not None:
                summary = work.summary_text
            elif (
                work.summary
                and work.summary.representation
                and work.summary.representation.content
            ):
                content = work.summary.representation.content.decode("utf-8")
                work.summary_text = content
                summary = work.summary_text
        return summary


class Annotator(ToFeedEntry):
    @staticmethod
    def _sample_link_comparator(link1: Link, link2: Link) -> int:
        link_1_type = link1.type
        link_2_type = link2.type
        assert link_1_type
        assert link_2_type

        epub_type = MediaTypes.EPUB_MEDIA_TYPE

        if link_1_type == epub_type and link_2_type != epub_type:
            return -1
        elif link_1_type != epub_type and link_2_type == epub_type:
            return 1
        else:
            return 0

    def annotate_work_entry(
        self, entry: WorkEntry, updated: datetime.datetime | None = None
    ) -> None:
        """
        Any data that the serializer must consider while generating an "entry"
        must be populated in this method.
        The serializer may not use all the data populated based on the protocol it is bound to.
        """
        if entry.computed:
            return

        work = entry.work
        edition = entry.edition
        identifier = entry.identifier
        pool = entry.license_pool
        computed = WorkEntryData()

        image_links = []
        other_links = []
        for rel, url in [
            (Hyperlink.IMAGE, work.cover_full_url),
            (Hyperlink.THUMBNAIL_IMAGE, work.cover_thumbnail_url),
        ]:
            if not url:
                continue
            image_type = "image/png"
            if url.endswith(".jpeg") or url.endswith(".jpg"):
                image_type = "image/jpeg"
            elif url.endswith(".gif"):
                image_type = "image/gif"
            image_links.append(Link(rel=rel, href=url, type=image_type))

        samples = self.samples(edition)
        for sample in samples:
            representation = sample.resource.representation
            media_type = representation.media_type if representation else None
            if not sample.resource.url:
                continue
            other_links.append(
                Link(
                    rel=Hyperlink.CLIENT_SAMPLE,
                    href=sample.resource.url,
                    type=media_type,
                )
            )

        other_links.sort(key=cmp_to_key(self._sample_link_comparator))

        if edition.medium:
            additional_type = Edition.medium_to_additional_type.get(str(edition.medium))
            if not additional_type:
                logging.warning("No additionalType for medium %s", edition.medium)
            computed.additional_type = additional_type

        computed.title = edition.title or OPDSFeed.NO_TITLE

        if edition.subtitle:
            computed.subtitle = edition.subtitle
        if edition.sort_title:
            computed.sort_title = edition.sort_title

        author_entries = self.authors(edition)
        computed.contributors = author_entries.get("contributors", [])
        computed.authors = author_entries.get("authors", [])

        if edition.series:
            computed.series = self.series(edition.series, edition.series_position)

        if edition.duration is not None:
            computed.duration = float(edition.duration)

        content = self.content(work)
        if content:
            computed.summary = RichText(text=content, content_type="html")

        computed.pwid = edition.permanent_work_id

        categories_by_scheme = self.categories(work)
        category_tags = []
        for scheme, categories in list(categories_by_scheme.items()):
            for category in categories:
                category = dict(
                    list(map(str, (k, v))) for k, v in list(category.items())
                )
                rating_value = category.get("ratingValue")
                category_tag = Category(
                    scheme=scheme,
                    term=category.get("term", ""),
                    label=category.get("label", ""),
                    rating_value=(
                        str(rating_value) if rating_value is not None else None
                    ),
                )
                category_tags.append(category_tag)
        computed.categories = category_tags

        if edition.language_code:
            computed.language = edition.language_code

        if edition.publisher:
            computed.publisher = edition.publisher

        if edition.imprint:
            computed.imprint = edition.imprint

        if edition.issued or edition.published:
            computed.issued = edition.issued or edition.published

        if identifier:
            computed.identifier = identifier.urn

        if pool:
            data_source = pool.data_source.active_name
            if data_source != DataSource.INTERNAL_PROCESSING:
                # INTERNAL_PROCESSING indicates a dummy LicensePool
                # created as a stand-in, e.g. by the metadata wrangler.
                # This component is not actually distributing the book,
                # so it should not have a bibframe:distribution tag.
                computed.distribution = Distribution(provider_name=data_source)

            # We use Atom 'published' for the date the book first became
            # available to people using this application.
            avail = pool.availability_time
            if avail:
                today = datetime.date.today()
                if isinstance(avail, datetime.datetime):
                    avail_date = avail.date()
                else:
                    avail_date = avail  # type: ignore[unreachable]
                if avail_date <= today:  # Avoid obviously wrong values.
                    computed.published = strftime(avail_date)

        if not updated and entry.work.last_update_time:
            updated = entry.work.last_update_time
        if updated:
            computed.updated = strftime(updated)

        computed.image_links = image_links
        computed.other_links = other_links
        entry.computed = computed

    def annotate_feed(self, feed: FeedData) -> None:
        """Any additional metadata or links that should be added to the feed (not each entry)
        should be added to the FeedData object in this method.
        """

    def active_licensepool_for(
        self, work: Work, library: Library | None = None
    ) -> LicensePool | None:
        """Which license pool would be/has been used to issue a license for
        this work?
        """
        if not work:
            return None

        return work.active_license_pool(library=library)
