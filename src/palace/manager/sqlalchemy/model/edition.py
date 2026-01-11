# Edition
from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, Query, relationship
from sqlalchemy.orm.session import Session

from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.sqlalchemy.constants import (
    DataSourceConstants,
    EditionConstants,
    LinkRelations,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util import MetadataSimilarity, TitleProcessor
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.permanent_work_id import WorkIDCalculator

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.customlist import CustomListEntry
    from palace.manager.sqlalchemy.model.resource import Resource
    from palace.manager.sqlalchemy.model.work import Work


class Edition(Base, EditionConstants):
    """A lightly schematized collection of metadata for a work, or an
    edition of a work, or a book, or whatever. If someone thinks of it
    as a "book" with a "title" it can go in here.
    """

    __tablename__ = "editions"
    id: Mapped[int] = Column(Integer, primary_key=True)

    data_source_id: Mapped[int] = Column(
        Integer, ForeignKey("datasources.id"), index=True, nullable=False
    )
    data_source: Mapped[DataSource] = relationship(
        "DataSource", back_populates="editions"
    )

    MAX_THUMBNAIL_HEIGHT = 300
    MAX_THUMBNAIL_WIDTH = 200

    # A full-sized image no larger than this height can be used as a thumbnail
    # in a pinch.
    MAX_FALLBACK_THUMBNAIL_HEIGHT = 500

    # Postgresql doesn't allow indices to exceed 1/3 of a buffer page.
    # We saw the following error here: https://ebce-lyrasis.atlassian.net/browse/PP-188:
    #
    # Index row size 3208 exceeds btree version 4 maximum 2704 for index "ix_editions_author"
    # DETAIL:  Index row references tuple (48187,9) in relation "editions".
    # HINT:  Values larger than 1/3 of a buffer page cannot be indexed.
    #
    # On rare occasions the author (and sort_author) fields can contain a concatenated list of a
    # large number of authors which breaks the index and causes failures.  What exactly that threshold is
    # I am not entirely certain.  It appears that 2704 is the size that broke the 1/3 of a buffer page
    # limit. However, I'm not sure how the index size is calculated. I experimented
    # with different values.  Author field values exceeding 2700 characters in length produced the aforementioned
    # error with an index row size of 2800.  Author field values below 2650 characters seemed to be okay.
    SAFE_AUTHOR_FIELD_LENGTH_TO_AVOID_PG_INDEX_ERROR = 2650

    # This Edition is associated with one particular
    # identifier--the one used by its data source to identify
    # it. Through the Equivalency class, it is associated with a
    # (probably huge) number of other identifiers.
    primary_identifier_id = Column(
        Integer, ForeignKey("identifiers.id"), index=True, nullable=False
    )
    primary_identifier: Mapped[Identifier] = relationship(
        "Identifier", back_populates="primarily_identifies"
    )

    # An Edition may be the presentation edition for a single Work. If it's not
    # a presentation edition for a work, work will be None.
    work: Mapped[Work | None] = relationship(
        "Work", uselist=False, back_populates="presentation_edition"
    )

    # An Edition may show up in many CustomListEntries.
    custom_list_entries: Mapped[list[CustomListEntry]] = relationship(
        "CustomListEntry", back_populates="edition"
    )

    # An Edition may be the presentation edition for many LicensePools.
    is_presentation_for: Mapped[list[LicensePool]] = relationship(
        "LicensePool", back_populates="presentation_edition"
    )

    title = Column(Unicode, index=True)
    sort_title = Column(Unicode, index=True)
    subtitle = Column(Unicode, index=True)
    series = Column(Unicode, index=True)
    series_position = Column(Integer)

    # This is not a foreign key per se; it's a calculated UUID-like
    # identifier for this work based on its title and author, used to
    # group together different editions of the same work.
    permanent_work_id = Column(String(36), index=True)

    # A string depiction of the authors' names.
    author = Column(Unicode, index=True)
    sort_author = Column(Unicode, index=True)

    contributions: Mapped[list[Contribution]] = relationship(
        "Contribution", back_populates="edition", uselist=True
    )

    language = Column(Unicode, index=True)
    publisher = Column(Unicode, index=True)
    imprint = Column(Unicode, index=True)

    # `issued` is the date the ebook edition was sent to the distributor by the publisher,
    # i.e. the date it became available for librarians to buy for their libraries
    issued = Column(Date)
    # `published is the original publication date of the text.
    # A Project Gutenberg text was likely `published` long before being `issued`.
    published = Column(Date)

    medium = Column(Enum(*EditionConstants.KNOWN_MEDIA, name="medium"), index=True)

    # The playtime duration of an audiobook (seconds)
    # https://github.com/readium/webpub-manifest/tree/master/contexts/default#duration-and-number-of-pages
    duration = Column(Float, nullable=True)

    cover_id = Column(
        Integer,
        ForeignKey("resources.id", use_alter=True, name="fk_editions_summary_id"),
        index=True,
    )
    cover: Mapped[Resource | None] = relationship(
        "Resource", back_populates="cover_editions", foreign_keys=[cover_id]
    )
    # These two let us avoid actually loading up the cover Resource
    # every time.
    cover_full_url = Column(Unicode)
    cover_thumbnail_url = Column(Unicode)

    # Information kept in here probably won't be used.
    extra: Mapped[dict[str, str]] = Column(
        MutableDict.as_mutable(JSON), default={}, nullable=False
    )

    # Timestamps to let us know when this item was created in our database, and when it was last updated.
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=None)

    def __repr__(self) -> str:
        id_repr = repr(self.primary_identifier)
        return "Edition {} [{!r}] ({}/{}/{})".format(
            self.id,
            id_repr,
            self.title,
            ", ".join([x.sort_name for x in self.contributors]),
            self.language,
        )

    @property
    def language_code(self) -> str | None:
        """A single BCP47 language code for display purposes."""
        if not self.language:
            return None
        result: str | None = LanguageCodes.bcp47_for_locale(
            self.language, default=self.language
        )
        return result

    @property
    def contributors(self) -> set[Contributor]:
        return {x.contributor for x in self.contributions}

    @property
    def author_contributors(self) -> list[Contributor]:
        """All distinct 'author'-type contributors, with the primary author
        first, other authors sorted by sort name.
        Basically, we're trying to figure out what would go on the
        book cover. The primary author should go first, and be
        followed by non-primary authors in alphabetical order. People
        whose role does not rise to the level of "authorship"
        (e.g. author of afterword) do not show up.
        The list as a whole should contain no duplicates. This might
        happen because someone is erroneously listed twice in the same
        role, someone is listed as both primary author and regular
        author, someone is listed as both author and translator,
        etc. However it happens, your name only shows up once on the
        front of the book.
        """
        seen_authors: set[Contributor] = set()
        primary_author: Contributor | None = None
        other_authors: list[Contributor] = []
        acceptable_substitutes: defaultdict[str, list[Contributor]] = defaultdict(list)
        if not self.contributions:
            return []

        # If there is one and only one contributor, return them, no
        # matter what their role is.
        if len(self.contributions) == 1:
            return [self.contributions[0].contributor]

        # There is more than one contributor. Try to pick out the ones
        # that rise to the level of being 'authors'.
        for x in self.contributions:
            if not primary_author and x.role == Contributor.Role.PRIMARY_AUTHOR:
                primary_author = x.contributor
            elif x.role in Contributor.AUTHOR_ROLES:
                other_authors.append(x.contributor)
            elif x.role.lower().startswith("author and"):
                other_authors.append(x.contributor)
            elif (
                x.role in Contributor.AUTHOR_SUBSTITUTE_ROLES
                or x.role in Contributor.PERFORMER_ROLES
            ):
                l = acceptable_substitutes[x.role]
                if x.contributor not in l:
                    l.append(x.contributor)

        def dedupe(contributors: list[Contributor]) -> list[Contributor]:
            """If an item shows up multiple times in a list,
            keep only the first occurence.
            """
            seen: set[Contributor] = set()
            deduped: list[Contributor] = []
            for i in contributors:
                if i in seen:
                    continue
                deduped.append(i)
                seen.add(i)
            return deduped

        if primary_author:
            return dedupe(
                [primary_author] + sorted(other_authors, key=lambda x: x.sort_name)
            )

        if other_authors:
            return dedupe(other_authors)

        for role in Contributor.AUTHOR_SUBSTITUTE_ROLES + Contributor.PERFORMER_ROLES:
            if role in acceptable_substitutes:
                contributors = acceptable_substitutes[role]
                return dedupe(sorted(contributors, key=lambda x: x.sort_name))
        else:
            # There are roles, but they're so random that we can't be
            # sure who's the 'author' or so low on the creativity
            # scale (like 'Executive producer') that we just don't
            # want to put them down as 'author'.
            return []

    @classmethod
    def medium_from_media_type(cls, media_type: str | None) -> str | None:
        """Derive a value for Edition.medium from a media type.

        TODO: It's not necessary right now, but we could theoretically
        derive this information from some other types such as
        our internal types for Overdrive manifests.

        :param media_type: A media type with optional parameters
        :return: A value for Edition.medium.
        """
        if not media_type:
            return None

        for types, conclusion in (
            (MediaTypes.AUDIOBOOK_MEDIA_TYPES, Edition.AUDIO_MEDIUM),
            (MediaTypes.BOOK_MEDIA_TYPES, Edition.BOOK_MEDIUM),
        ):
            if any(media_type.startswith(x) for x in types):
                return conclusion

        if media_type.startswith(DeliveryMechanism.ADOBE_DRM):
            # Adobe DRM is only applied to ebooks.
            return Edition.BOOK_MEDIUM
        return None

    @classmethod
    @overload
    def for_foreign_id(
        cls,
        _db: Session,
        data_source: DataSource | str | None,
        foreign_id_type: str,
        foreign_id: str,
        autocreate: Literal[True] = ...,
    ) -> tuple[Edition, bool]: ...

    @classmethod
    @overload
    def for_foreign_id(
        cls,
        _db: Session,
        data_source: DataSource | str | None,
        foreign_id_type: str,
        foreign_id: str,
        autocreate: bool,
    ) -> tuple[Edition | None, bool]: ...

    @classmethod
    def for_foreign_id(
        cls,
        _db: Session,
        data_source: DataSource | str | None,
        foreign_id_type: str,
        foreign_id: str,
        autocreate: bool = True,
    ) -> tuple[Edition | None, bool]:
        """Find the Edition representing the given data source's view of
        the work that it primarily identifies by foreign ID.
        e.g. for_foreign_id(_db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, uuid)
        finds the Edition for Overdrive's view of a book identified
        by Overdrive UUID.
        This:
        for_foreign_id(_db, DataSource.OVERDRIVE, Identifier.ISBN, isbn)
        will probably return nothing, because although Overdrive knows
        that books have ISBNs, it doesn't use ISBN as a primary
        identifier.

        """
        # Look up the data source if necessary.
        if isinstance(data_source, str):
            data_source = DataSource.lookup(_db, data_source, autocreate=autocreate)
        if data_source is None:
            return None, False

        identifier, ignore = Identifier.for_foreign_id(
            _db, foreign_id_type, foreign_id, autocreate=autocreate
        )
        if identifier is None:
            return None, False

        if not autocreate:
            is_new = False
            edition = get_one(
                _db,
                Edition,
                data_source=data_source,
                primary_identifier=identifier,
            )
        else:
            edition, is_new = get_one_or_create(
                _db,
                Edition,
                data_source=data_source,
                primary_identifier=identifier,
            )

        return edition, is_new

    @property
    def license_pools(self) -> list[LicensePool]:
        """The LicensePools that provide access to the book described
        by this Edition.
        """
        _db = Session.object_session(self)
        return (
            _db.query(LicensePool)
            .filter(
                LicensePool.data_source == self.data_source,
                LicensePool.identifier == self.primary_identifier,
            )
            .all()
        )

    def equivalent_identifiers(
        self,
        type: str | list[str] | None = None,
        policy: PresentationCalculationPolicy | None = None,
    ) -> list[Identifier]:
        """All Identifiers equivalent to this
        Edition's primary identifier, according to the given
        PresentationCalculationPolicy
        """
        _db = Session.object_session(self)
        identifier_id_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            self.primary_identifier.id, policy=policy
        )
        q = _db.query(Identifier).filter(Identifier.id.in_(identifier_id_subquery))
        if type:
            if isinstance(type, list):
                q = q.filter(Identifier.type.in_(type))
            else:
                q = q.filter(Identifier.type == type)
        return q.all()

    def equivalent_editions(
        self, policy: PresentationCalculationPolicy | None = None
    ) -> Query[Edition]:
        """All Editions whose primary ID is equivalent to this Edition's
        primary ID, according to the given PresentationCalculationPolicy.
        """
        _db = Session.object_session(self)
        identifier_id_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            self.primary_identifier.id, policy=policy
        )
        return _db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_id_subquery)
        )

    @classmethod
    def sort_by_priority(
        cls, editions: list[Edition], license_source: DataSource | None = None
    ) -> list[Edition]:
        """Return all Editions that describe the Identifier associated with
        this LicensePool, in the order they should be used to create a
        presentation Edition for the LicensePool.
        """

        def sort_key(edition: Edition) -> float:
            """Return a numeric ordering of this edition."""
            source = edition.data_source
            if not source:
                # This shouldn't happen. Give this edition the
                # lowest priority.
                return -100

            if source == license_source:
                # This Edition contains information from the same data
                # source as the LicensePool itself. Put it below any
                # Edition from one of the data sources in
                # PRESENTATION_EDITION_PRIORITY, but above all other
                # Editions.
                return -1

            elif source.name == DataSourceConstants.METADATA_WRANGLER:
                # The metadata wrangler is slightly less trustworthy
                # than the license source, for everything except cover
                # image (which is handled by
                # Representation.quality_as_thumbnail_image)
                return -1.5

            if source.name in DataSourceConstants.PRESENTATION_EDITION_PRIORITY:
                return DataSourceConstants.PRESENTATION_EDITION_PRIORITY.index(
                    source.name
                )
            else:
                return -2

        return sorted(editions, key=sort_key)

    @classmethod
    def _content(
        cls, content: str | None, is_html: bool = False
    ) -> dict[str, str] | None:
        """Represent content that might be plain-text or HTML.
        e.g. a book's summary.
        """
        if not content:
            return None
        if is_html:
            content_type = "html"
        else:
            content_type = "text"
        return dict(type=content_type, value=content)

    def set_cover(self, resource: Resource) -> None:
        old_cover = self.cover
        old_cover_full_url = self.cover_full_url
        old_cover_thumbnail_url = self.cover_thumbnail_url
        new_cover = resource
        if (representation := resource.representation) is None:
            return
        new_cover_full_url = representation.public_url
        new_cover_thumbnail_url: str | None = None
        # TODO: In theory there could be multiple scaled-down
        # versions of this representation and we need some way of
        # choosing between them. Right now we just pick the first one
        # that works.
        if (
            representation.image_height
            and representation.image_height <= self.MAX_THUMBNAIL_HEIGHT
        ):
            # This image doesn't need a thumbnail.
            new_cover_thumbnail_url = representation.public_url
        else:
            # Use the best available thumbnail for this image.
            best_thumbnail = representation.best_thumbnail
            if best_thumbnail:
                new_cover_thumbnail_url = best_thumbnail.public_url
        if (
            not new_cover_thumbnail_url
            and representation.image_height
            and representation.image_height <= self.MAX_FALLBACK_THUMBNAIL_HEIGHT
        ):
            # The full-sized image is too large to be a thumbnail, but it's
            # not huge, and there is no other thumbnail, so use it.
            new_cover_thumbnail_url = representation.public_url
        if (
            old_cover != new_cover
            or old_cover_full_url != new_cover_full_url
            or old_cover_thumbnail_url != new_cover_thumbnail_url
        ):
            self.cover = new_cover
            self.cover_full_url = new_cover_full_url
            self.cover_thumbnail_url = new_cover_thumbnail_url

            logging.debug(
                "Setting cover for %s/%s: full=%s thumb=%s",
                self.primary_identifier.type,
                self.primary_identifier.identifier,
                self.cover_full_url,
                self.cover_thumbnail_url,
            )

    def add_contributor(
        self,
        name: Contributor | str | None,
        roles: Sequence[str] | str,
        aliases: Sequence[str] | None = None,
        lc: str | None = None,
        viaf: str | None = None,
    ) -> Contributor:
        """Assign a contributor to this Edition."""
        _db = Session.object_session(self)
        if isinstance(roles, str):
            roles = [roles]

        # First find or create the Contributor.
        if isinstance(name, Contributor):
            contributor = name
        else:
            contributor, _ = Contributor.lookup(_db, name, lc, viaf, aliases)
            if isinstance(contributor, list):
                # Contributor was looked up/created by name,
                # which returns a list.
                contributor = contributor[0]

        # Then add their Contributions.
        for role in roles:
            get_one_or_create(
                _db, Contribution, edition=self, contributor=contributor, role=role
            )
        return contributor

    def similarity_to(self, other_record: Edition) -> float:
        """How likely is it that this record describes the same book as the
        given record?
        1 indicates very strong similarity, 0 indicates no similarity
        at all.
        For now we just compare the sets of words used in the titles
        and the authors' names. This should be good enough for most
        cases given that there is usually some preexisting reason to
        suppose that the two records are related (e.g. OCLC said
        they were).
        Most of the Editions are from OCLC Classify, and we expect
        to get some of them wrong (e.g. when a single OCLC work is a
        compilation of several novels by the same author). That's okay
        because those Editions aren't backed by
        LicensePools. They're purely informative. We will have some
        bad information in our database, but the clear-cut cases
        should outnumber the fuzzy cases, so we we should still group
        the Editions that really matter--the ones backed by
        LicensePools--together correctly.
        TODO: apply much more lenient terms if the two Editions are
        identified by the same ISBN or other unique identifier.
        """
        if other_record == self:
            # A record is always identical to itself.
            return 1

        language_factor: float
        if other_record.language == self.language:
            # The books are in the same language. Hooray!
            language_factor = 1
        else:
            if other_record.language and self.language:
                # Each record specifies a different set of languages. This
                # is an immediate disqualification.
                return 0
            else:
                # One record specifies a language and one does not. This
                # is a little tricky. We're going to apply a penalty, but
                # since the majority of records we're getting from OCLC are in
                # English, the penalty will be less if one of the
                # languages is English. It's more likely that an unlabeled
                # record is in English than that it's in some other language.
                if self.language == "eng" or other_record.language == "eng":
                    language_factor = 0.80
                else:
                    language_factor = 0.50

        title_quotient = MetadataSimilarity.title_similarity(
            self.title, other_record.title
        )

        author_quotient = MetadataSimilarity.author_similarity(
            self.author_contributors, other_record.author_contributors
        )
        if author_quotient == 0:
            # The two works have no authors in common. Immediate
            # disqualification.
            return 0

        # We weight title more heavily because it's much more likely
        # that one author wrote two different books than that two
        # books with the same title have different authors.
        result: float = language_factor * (
            (title_quotient * 0.80) + (author_quotient * 0.20)
        )
        return result

    def apply_similarity_threshold(
        self, candidates: list[Edition], threshold: float = 0.5
    ) -> Iterator[Edition]:
        """Yield the Editions from the given list that are similar
        enough to this one.
        """
        for candidate in candidates:
            if self == candidate:
                yield candidate
            else:
                similarity = self.similarity_to(candidate)
                if similarity >= threshold:
                    yield candidate

    def best_cover_within_distance(
        self,
        distance: int,
        rel: str | None = None,
        policy: PresentationCalculationPolicy | None = None,
    ) -> tuple[Resource | None, list[Resource]]:
        _db = Session.object_session(self)
        identifier_ids: list[int] = [self.primary_identifier.id]

        if distance > 0:
            if policy is None:
                new_policy = PresentationCalculationPolicy()
            else:
                new_policy = PresentationCalculationPolicy(
                    equivalent_identifier_levels=distance,
                    equivalent_identifier_cutoff=policy.equivalent_identifier_cutoff,
                    equivalent_identifier_threshold=policy.equivalent_identifier_threshold,
                )

            identifier_ids_dict = Identifier.recursively_equivalent_identifier_ids(
                _db, identifier_ids, policy=new_policy
            )
            identifier_ids += identifier_ids_dict[self.primary_identifier.id]

        return Identifier.best_cover_for(_db, identifier_ids, rel=rel)

    @property
    def title_for_permanent_work_id(self) -> str | None:
        title = self.title
        if self.subtitle:
            title = (title or "") + ": " + self.subtitle
        return title

    @property
    def author_for_permanent_work_id(self) -> str | None:
        authors = self.author_contributors
        author: str | None
        if authors:
            # Use the sort name of the primary author.
            author = authors[0].sort_name
        else:
            # This may be an Edition that represents an item on a best-seller list
            # or something like that. In this case it wouldn't have any Contributor
            # objects, just an author string. Use that.
            author = self.sort_author or self.author
        return author

    def calculate_permanent_work_id(self, debug: bool = False) -> None:
        title = self.title_for_permanent_work_id
        medium: str | None = (
            self.medium_for_permanent_work_id.get(self.medium) if self.medium else None
        )
        if not title or not medium:
            # If a book has no title or medium, it has no permanent work ID.
            if self.permanent_work_id != None:
                self.permanent_work_id = None
            return

        author = self.author_for_permanent_work_id

        w = WorkIDCalculator
        norm_title = w.normalize_title(title)
        norm_author = w.normalize_author(author)

        old_id = self.permanent_work_id
        new_permanent_work_id = self.calculate_permanent_work_id_for_title_and_author(
            title, author, medium
        )

        if old_id != new_permanent_work_id:
            self.permanent_work_id = new_permanent_work_id

        args = (
            "Permanent work ID for %d: %s/%s -> %s/%s/%s -> %s (was %s)",
            self.id,
            title,
            author,
            norm_title,
            norm_author,
            medium,
            new_permanent_work_id,
            old_id,
        )
        if debug:
            logging.debug(*args)
        elif old_id != self.permanent_work_id:
            logging.info(*args)

    @classmethod
    def calculate_permanent_work_id_for_title_and_author(
        cls, title: str, author: str | None, medium: str
    ) -> str:
        w = WorkIDCalculator
        norm_title = w.normalize_title(title)
        norm_author = w.normalize_author(author)

        result: str = WorkIDCalculator.permanent_id(norm_title, norm_author, medium)
        return result

    UNKNOWN_AUTHOR = "[Unknown]"

    def calculate_presentation(
        self, policy: PresentationCalculationPolicy | None = None
    ) -> bool:
        """Make sure the presentation of this Edition is up-to-date."""
        _db = Session.object_session(self)
        changed = False
        if policy is None:
            policy = PresentationCalculationPolicy()

        # Gather information up front that will be used to determine
        # whether this method actually did anything.
        old_author = self.author
        old_sort_author = self.sort_author
        old_sort_title = self.sort_title
        old_work_id = self.permanent_work_id
        old_cover = self.cover
        old_cover_full_url = self.cover_full_url
        old_cover_thumbnail_url = self.cover_thumbnail_url

        if policy.set_edition_metadata:
            new_author, new_sort_author = self.calculate_author()
            new_sort_title = TitleProcessor.sort_title_for(self.title)

            if old_author != new_author:
                self.author = new_author
            if old_sort_author != new_sort_author:
                self.sort_author = new_sort_author
            if old_sort_title != new_sort_title:
                self.sort_title = new_sort_title

            self.calculate_permanent_work_id()

        if policy.choose_cover:
            self.choose_cover(policy=policy)

        if (
            self.author != old_author
            or self.sort_author != old_sort_author
            or self.sort_title != old_sort_title
            or self.permanent_work_id != old_work_id
            or self.cover != old_cover
            or self.cover_full_url != old_cover_full_url
            or self.cover_thumbnail_url != old_cover_thumbnail_url
        ):
            changed = True

        # Now that everything's calculated, log it.
        if policy.verbose:
            if changed:
                changed_status = "changed"
                level = logging.info
            else:
                changed_status = "unchanged"
                level = logging.debug

            msg = "Presentation %s for Edition %s (by %s, pub=%s, ident=%s/%s, pwid=%s, language=%s, cover=%r)"
            args = [
                changed_status,
                self.title,
                self.author,
                self.publisher,
                self.primary_identifier.type,
                self.primary_identifier.identifier,
                self.permanent_work_id,
                self.language,
            ]
            if self.cover and self.cover.representation:
                args.append(self.cover.representation.public_url)
            else:
                args.append(None)
            level(msg, *args)
        return changed

    def calculate_author(self) -> tuple[str, str]:
        """Turn the list of Contributors into string values for .author
        and .sort_author.
        """

        sort_names: list[str] = []
        display_names: list[list[str]] = []
        for contributor in self.author_contributors:
            default_family: str | None = None
            default_display: str | None = None
            if (
                contributor.sort_name
                and not contributor.display_name
                or not contributor.family_name
            ):
                default_family, default_display = contributor.default_names()
            display_name = (
                contributor.display_name or default_display or contributor.sort_name
            )
            family_name = (
                contributor.family_name or default_family or contributor.sort_name
            )
            display_names.append([family_name or "", display_name or ""])
            sort_names.append(contributor.sort_name or "")
        if display_names:
            author = ", ".join([x[1] for x in sorted(display_names)])
        else:
            author = self.UNKNOWN_AUTHOR
        if sort_names:
            sort_author = " ; ".join(sorted(sort_names))
        else:
            sort_author = self.UNKNOWN_AUTHOR

        def truncate_string(mystr: str) -> str:
            if len(mystr) > self.SAFE_AUTHOR_FIELD_LENGTH_TO_AVOID_PG_INDEX_ERROR:
                return (
                    mystr[: (self.SAFE_AUTHOR_FIELD_LENGTH_TO_AVOID_PG_INDEX_ERROR - 3)]
                    + "..."
                )
            return mystr

        # Very long author and sort_author strings can cause issues for Postgres indices. See
        # comment above the SAFE_AUTHOR_FIELD_LENGTH_TO_AVOID_PG_INDEX_ERROR constant for details.
        author = truncate_string(author)
        sort_author = truncate_string(sort_author)
        return author, sort_author

    def choose_cover(self, policy: PresentationCalculationPolicy | None = None) -> None:
        """Try to find a cover that can be used for this Edition."""

        for distance in (0, 5):
            # If there's a cover directly associated with the
            # Edition's primary ID, use it. Otherwise, find the
            # best cover associated with any related identifier.
            best_cover, covers = self.best_cover_within_distance(
                distance=distance, policy=policy
            )

            if best_cover:
                if not best_cover.representation:
                    logging.warning(
                        "Best cover for %r has no representation!",
                        self.primary_identifier,
                    )
                else:
                    rep = best_cover.representation
                    if not rep.thumbnails:
                        logging.warning(
                            "Best cover for %r (%s) was never thumbnailed!",
                            self.primary_identifier,
                            rep.public_url,
                        )
                self.set_cover(best_cover)
                break
        else:
            # No cover has been found. If the Edition currently references
            # a cover, it has since been rejected or otherwise removed.
            # Cover details need to be removed.
            cover_info = [self.cover, self.cover_full_url, self.cover_thumbnail_url]
            if any(cover_info):
                self.cover = None
                self.cover_full_url = None
                self.cover_thumbnail_url = None

        if not self.cover_thumbnail_url:
            # The process we went through above did not result in the
            # setting of a thumbnail cover.
            #
            # It's possible there's a thumbnail even when there's no
            # full-sized cover, or when the full-sized cover and
            # thumbnail are different Resources on the same
            # Identifier. Try to find a thumbnail the same way we'd
            # look for a cover.
            for distance in (0, 5):
                best_thumbnail, thumbnails = self.best_cover_within_distance(
                    distance=distance,
                    policy=policy,
                    rel=LinkRelations.THUMBNAIL_IMAGE,
                )
                if best_thumbnail:
                    if not best_thumbnail.representation:
                        logging.warning(
                            "Best thumbnail for %r has no representation!",
                            self.primary_identifier,
                        )
                    else:
                        rep = best_thumbnail.representation
                        self.cover_thumbnail_url = rep.public_url
                    break

        # After exhausting all options for finding a thumbnail, fall back to
        # using the full-size image as thumbnail if we have a cover but no thumbnail.
        if not self.cover_thumbnail_url and self.cover_full_url:
            # We don't have any thumbnail, so fall back to the full size image.
            # TODO: We may want to scale and generate a thumbnail in this case, instead
            #   of serving up the full-size image as a thumbnail.
            logging.info(
                f"Using full-size image as thumbnail fallback for {self.primary_identifier!r}",
            )
            self.cover_thumbnail_url = self.cover_full_url


Index(
    "ix_editions_data_source_id_identifier_id",
    Edition.data_source_id,
    Edition.primary_identifier_id,
    unique=True,
)
