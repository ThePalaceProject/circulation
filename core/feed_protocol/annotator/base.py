from __future__ import annotations

import datetime
import logging
from collections import defaultdict
from urllib.parse import quote

from flask import has_request_context
from flask import url_for as flask_url_for
from sqlalchemy.orm import Session, joinedload

from core.classifier import Classifier
from core.feed_protocol.types import (
    Author,
    FeedData,
    FeedEntryType,
    Link,
    WorkEntry,
    WorkEntryData,
)
from core.model.classification import Subject
from core.model.contributor import Contributor
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.library import Library
from core.model.licensing import LicensePool
from core.model.resource import Hyperlink
from core.model.work import Work
from core.util.datetime_helpers import utc_now
from core.util.opds_writer import AtomFeed, OPDSFeed


class ToFeedEntry:
    @classmethod
    def authors(cls, edition):
        """Create one or more <author> and <contributor> tags for the given
        Work.

        :param work: The Work under consideration.
        :param edition: The Edition to use as a reference
            for bibliographic information, including the list of
            Contributions.
        """
        authors = {"authors": [], "contributors": []}
        state = defaultdict(set)
        for contribution in edition.contributions:
            info = cls.contributor_tag(contribution, state)
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
    def contributor_tag(cls, contribution, state):
        """Build an <author> or <contributor> tag for a Contribution.

        :param contribution: A Contribution.
        :param state: A defaultdict of sets, which may be used to keep
            track of what happened during previous calls to
            contributor_tag for a given Work.
        :return: A Tag, or None if creating a Tag for this Contribution
            would be redundant or of low value.

        """
        contributor = contribution.contributor
        role = contribution.role
        entries = {}

        if role in Contributor.AUTHOR_ROLES:
            tag_f = "author"
            marc_role = None
        else:
            tag_f = "contributor"
            marc_role = Contributor.MARC_ROLE_CODES.get(role)
            if not marc_role:
                # This contribution is not one that we publish as
                # a <atom:contributor> tag. Skip it.
                return None

        name = contributor.display_name or contributor.sort_name
        name_key = name.lower()
        if name_key in state[marc_role]:
            # We've already credited this person with this
            # MARC role. Returning a tag would be redundant.
            return None

        # Okay, we're creating a tag.
        properties = dict()
        if marc_role:
            properties["role"] = marc_role
        entry = Author(name=name, **properties)

        # Record the fact that we credited this person with this role,
        # so that we don't do it again on a subsequent call.
        state[marc_role].add(name_key)

        return tag_f, entry

    @classmethod
    def series(cls, series_name, series_position):
        """Generate a schema:Series tag for the given name and position."""
        if not series_name:
            return None
        series_details = dict()
        series_details["name"] = series_name
        if series_position != None:
            series_details["position"] = str(series_position)
        return FeedEntryType(**series_details)

    @classmethod
    def rating(cls, type_uri, value):
        """Generate a schema:Rating tag for the given type and value."""
        return FeedEntryType(ratingValue="%.4f" % value, additionalType=type_uri)

    @classmethod
    def samples(cls, edition: Edition) -> list[Hyperlink]:
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
    def categories(cls, work):
        """Return all relevant classifications of this work.

        :return: A dictionary mapping 'scheme' URLs to dictionaries of
            attribute-value pairs.

        Notable attributes: 'term', 'label', 'http://schema.org/ratingValue'
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
            ]

        # Add the appeals as a category of schema
        # http://librarysimplified.org/terms/appeal
        schema_url = AtomFeed.SIMPLIFIED_NS + "appeals/"
        appeals = []
        categories[schema_url] = appeals
        for name, value in (
            (Work.CHARACTER_APPEAL, work.appeal_character),
            (Work.LANGUAGE_APPEAL, work.appeal_language),
            (Work.SETTING_APPEAL, work.appeal_setting),
            (Work.STORY_APPEAL, work.appeal_story),
        ):
            if value:
                appeal = dict(term=schema_url + name, label=name)
                weight_field = AtomFeed.schema_("ratingValue")
                appeal[weight_field] = value
                appeals.append(appeal)

        # Add the audience as a category of schema
        # http://schema.org/audience
        if work.audience:
            audience_uri = AtomFeed.SCHEMA_NS + "audience"
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
    def content(cls, work):
        """Return an HTML summary of this work."""
        summary = ""
        if work:
            if work.summary_text != None:
                summary = work.summary_text
            elif work.summary and work.summary.content:
                work.summary_text = work.summary.content
                summary = work.summary_text
        return summary


def url_for(name, **kwargs):
    if has_request_context():
        return flask_url_for(name, **kwargs)
    else:
        params = "&".join([f"k=v" for k, v in kwargs.items()])
        return f"//{name}?{params}"


class OPDSAnnotator:
    def __init__(self, library) -> None:
        self.library = library


class Annotator(OPDSAnnotator, ToFeedEntry):
    def annotate_work_entry(self, entry: WorkEntry, updated=None) -> None:
        if entry.computed:
            return

        work = entry.work
        edition: Edition = entry.edition
        identifier = entry.identifier
        pool = entry.license_pool
        computed = WorkEntryData()

        # Everything from serializer should come here
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
            other_links.append(
                Link(
                    rel=Hyperlink.CLIENT_SAMPLE,
                    href=sample.resource.url,
                    type=sample.resource.representation.media_type,
                )
            )

        content = self.content(work)
        if isinstance(content, bytes):
            content = content.decode("utf8")

        if edition.medium:
            additional_type = Edition.medium_to_additional_type.get(str(edition.medium))
            if not additional_type:
                logging.warning("No additionalType for medium %s", edition.medium)
            computed.additionalType = additional_type

        computed.title = FeedEntryType(text=(edition.title or OPDSFeed.NO_TITLE))

        if edition.subtitle:
            computed.subtitle = FeedEntryType(text=edition.subtitle)

        # TODO: Is VerboseAnnotator used anywhere?

        author_entries = self.authors(edition)
        computed.contributors = author_entries.get("contributors")
        computed.authors = author_entries.get("authors")

        if edition.series:
            computed.series = self.series(edition.series, edition.series_position)

        if content:
            computed.summary = FeedEntryType(text=content, type="html")

        computed.pwid = edition.permanent_work_id

        categories_by_scheme = self.categories(work)
        category_tags = []
        for scheme, categories in list(categories_by_scheme.items()):
            for category in categories:
                if isinstance(category, (bytes, str)):
                    category = dict(term=category)
                category = dict(
                    list(map(str, (k, v))) for k, v in list(category.items())
                )
                category_tag = FeedEntryType(scheme=scheme, **category)
                category_tags.append(category_tag)
        computed.categories = category_tags

        if edition.language_code:
            computed.language = FeedEntryType(text=edition.language_code)

        if edition.publisher:
            computed.publisher = FeedEntryType(text=edition.publisher)

        if edition.imprint:
            computed.imprint = FeedEntryType(text=edition.imprint)

        # Entry.issued is the date the ebook came out, as distinct
        # from Entry.published (which may refer to the print edition
        # or some original edition way back when).
        #
        # For Dublin Core 'issued' we use Entry.issued if we have it
        # and Entry.published if not. In general this means we use
        # issued date for Gutenberg and published date for other
        # sources.
        #
        # For the date the book was added to our collection we use
        # atom:published.
        #
        # Note: feedparser conflates dc:issued and atom:published, so
        # it can't be used to extract this information. However, these
        # tags are consistent with the OPDS spec.
        issued = edition.issued or edition.published
        if isinstance(issued, datetime.datetime) or isinstance(issued, datetime.date):
            now = utc_now()
            today = datetime.date.today()
            issued_already = False
            if isinstance(issued, datetime.datetime):
                issued_already = issued <= now
            elif isinstance(issued, datetime.date):
                issued_already = issued <= today
            if issued_already:
                # Use datetime.isoformat instead of datetime.strftime because
                # strftime only works on dates after 1890, and we have works
                # that were issued much earlier than that.
                # TODO: convert to local timezone, not that it matters much.
                computed.issued = FeedEntryType(text=issued.isoformat().split("T")[0])

        if identifier:
            computed.identifier = identifier.urn

        if pool:
            data_source = pool.data_source.name
            if data_source != DataSource.INTERNAL_PROCESSING:
                # INTERNAL_PROCESSING indicates a dummy LicensePool
                # created as a stand-in, e.g. by the metadata wrangler.
                # This component is not actually distributing the book,
                # so it should not have a bibframe:distribution tag.
                kwargs = {"ProviderName": data_source}
                computed.distribution = FeedEntryType(**kwargs)

            # We use Atom 'published' for the date the book first became
            # available to people using this application.
            avail = pool.availability_time
            if avail:
                today = datetime.date.today()
                if isinstance(avail, datetime.datetime):
                    avail_date = avail.date()
                if avail_date <= today:  # Avoid obviously wrong values.
                    computed.published = FeedEntryType(
                        text=AtomFeed._strftime(avail_date)
                    )

        if not updated and entry.work.last_update_time:
            # NOTE: This is a default that works in most cases. When
            # ordering Opensearch results by last update time,
            # `work` is a WorkSearchResult object containing a more
            # reliable value that you can use if you want.
            updated = entry.work.last_update_time
        if updated:
            computed.updated = FeedEntryType(text=AtomFeed._strftime(updated))

        computed.image_links = image_links
        computed.other_links = other_links
        entry.computed = computed

    def annotate_feed(self, feed: FeedData):
        pass

    @classmethod
    def active_licensepool_for(
        cls, work: Work, library: Library | None = None
    ) -> LicensePool | None:
        """Which license pool would be/has been used to issue a license for
        this work?
        """
        if not work:
            return None

        return work.active_license_pool(library=library)
