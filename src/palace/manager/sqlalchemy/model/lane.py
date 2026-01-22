from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dependency_injector.wiring import Provide, inject
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    Table,
    Unicode,
    UniqueConstraint,
    or_,
)
from sqlalchemy.dialects.postgresql import ARRAY, INT4RANGE, JSON
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import (
    Mapped,
    relationship,
)
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import select

from palace.manager.core.classifier import Classifier
from palace.manager.core.entrypoint import EntryPoint, EverythingEntryPoint
from palace.manager.feed.facets.constants import FacetConstants
from palace.manager.feed.worklist.base import WorkList
from palace.manager.feed.worklist.database import DatabaseBackedWorkList
from palace.manager.feed.worklist.hierarchy import HierarchyWorkList
from palace.manager.feed.worklist.top_level import TopLevelWorkList
from palace.manager.sqlalchemy.hybrid import hybrid_property
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import (
    get_one_or_create,
    tuple_to_numericrange,
)
from palace.manager.util.languages import LanguageCodes

if TYPE_CHECKING:
    from palace.manager.search.external_search import (
        ExternalSearchIndex,
    )


class LaneGenre(Base):
    """Relationship object between Lane and Genre."""

    __tablename__ = "lanes_genres"
    id: Mapped[int] = Column(Integer, primary_key=True)
    lane_id: Mapped[int] = Column(
        Integer, ForeignKey("lanes.id"), index=True, nullable=False
    )
    lane: Mapped[Lane] = relationship("Lane", back_populates="lane_genres")
    genre_id: Mapped[int] = Column(
        Integer, ForeignKey("genres.id"), index=True, nullable=False
    )
    genre: Mapped[Genre] = relationship(Genre, back_populates="lane_genres")

    # An inclusive relationship means that books classified under the
    # genre are included in the lane. An exclusive relationship means
    # that books classified under the genre are excluded, even if they
    # would otherwise be included.
    inclusive: Mapped[bool] = Column(Boolean, default=True, nullable=False)

    # By default, this relationship applies not only to the genre
    # itself but to all of its subgenres. Setting recursive=false
    # means that only the genre itself is affected.
    recursive: Mapped[bool] = Column(Boolean, default=True, nullable=False)

    __table_args__ = (UniqueConstraint("lane_id", "genre_id"),)

    @classmethod
    def from_genre(cls, genre):
        """Used in the Lane.genres association proxy."""
        lg = LaneGenre()
        lg.genre = genre
        return lg


class Lane(Base, DatabaseBackedWorkList, HierarchyWorkList):
    """A WorkList that draws its search criteria from a row in a
    database table.

    A Lane corresponds roughly to a section in a branch library or
    bookstore. Lanes are the primary means by which patrons discover
    books.
    """

    # The set of Works in a standard Lane is cacheable for twenty
    # minutes. Note that this only applies to paginated feeds --
    # grouped feeds are cached indefinitely.
    MAX_CACHE_AGE = 20 * 60

    __tablename__ = "lanes"
    id: Mapped[int] = Column(Integer, primary_key=True)
    library_id: Mapped[int] = Column(
        Integer, ForeignKey("libraries.id"), index=True, nullable=False
    )
    library: Mapped[Library] = relationship(Library, back_populates="lanes")

    parent_id = Column(Integer, ForeignKey("lanes.id"), index=True, nullable=True)
    parent: Mapped[Lane | None] = relationship(
        "Lane",
        back_populates="sublanes",
        remote_side=[id],
    )

    priority: Mapped[int] = Column(Integer, index=True, nullable=False, default=0)

    # How many titles are in this lane? This is periodically
    # calculated and cached.
    size: Mapped[int] = Column(Integer, nullable=False, default=0)

    # How many titles are in this lane when viewed through a specific
    # entry point? This is periodically calculated and cached.
    size_by_entrypoint = Column(JSON, nullable=True)

    # A lane may have one parent lane and many sublanes.
    sublanes: Mapped[list[Lane]] = relationship(
        "Lane",
        back_populates="parent",
    )

    # A lane may have multiple associated LaneGenres. For most lanes,
    # this is how the contents of the lanes are defined.
    genres = association_proxy("lane_genres", "genre", creator=LaneGenre.from_genre)
    lane_genres: Mapped[list[LaneGenre]] = relationship(
        "LaneGenre",
        back_populates="lane",
        cascade="all, delete-orphan",
    )

    # display_name is the name of the lane as shown to patrons.  It's
    # okay for this to be duplicated within a library, but it's not
    # okay to have two lanes with the same parent and the same display
    # name -- that would be confusing.
    display_name: Mapped[str] = Column(Unicode, nullable=False)

    # True = Fiction only
    # False = Nonfiction only
    # null = Both fiction and nonfiction
    #
    # This may interact with lane_genres, for genres such as Humor
    # which can apply to either fiction or nonfiction.
    fiction = Column(Boolean, index=True, nullable=True)

    # A lane may be restricted to works classified for specific audiences
    # (e.g. only Young Adult works).
    _audiences = Column("audiences", ARRAY(Unicode))

    # A lane may further be restricted to works classified as suitable
    # for a specific age range.
    _target_age = Column("target_age", INT4RANGE, index=True)

    # A lane may be restricted to works available in certain languages.
    languages = Column(ARRAY(Unicode))

    # A lane may be restricted to works in certain media (e.g. only
    # audiobooks).
    media = Column(ARRAY(Unicode))

    # TODO: At some point it may be possible to restrict a lane to certain
    # formats (e.g. only electronic materials or only codices).

    # Only books licensed through this DataSource will be shown.
    license_datasource_id = Column(
        Integer, ForeignKey("datasources.id"), index=True, nullable=True
    )
    license_datasource: Mapped[DataSource | None] = relationship(
        "DataSource",
        back_populates="license_lanes",
        foreign_keys=[license_datasource_id],
    )

    # Only books on one or more CustomLists obtained from this
    # DataSource will be shown.
    _list_datasource_id = Column(
        Integer, ForeignKey("datasources.id"), index=True, nullable=True
    )
    _list_datasource: Mapped[DataSource | None] = relationship(
        "DataSource", back_populates="list_lanes", foreign_keys=[_list_datasource_id]
    )

    # Only the books on these specific CustomLists will be shown.
    customlists: Mapped[list[CustomList]] = relationship(
        "CustomList", secondary="lanes_customlists", back_populates="lane"
    )

    # This has no effect unless list_datasource_id or
    # list_identifier_id is also set. If this is set, then a book will
    # only be shown if it has a CustomListEntry on an appropriate list
    # where `most_recent_appearance` is within this number of days. If
    # the number is zero, then the lane contains _every_ book with a
    # CustomListEntry associated with an appropriate list.
    list_seen_in_previous_days = Column(Integer, nullable=True)

    # If this is set to True, then a book will show up in a lane only
    # if it would _also_ show up in its parent lane.
    inherit_parent_restrictions: Mapped[bool] = Column(
        Boolean, default=True, nullable=False
    )

    # Patrons whose external type is in this list will be sent to this
    # lane when they ask for the root lane.
    #
    # This is almost never necessary.
    root_for_patron_type = Column(ARRAY(Unicode), nullable=True)

    # A grouped feed for a Lane contains a swim lane from each
    # sublane, plus a swim lane at the bottom for the Lane itself. In
    # some cases that final swim lane should not be shown. This
    # generally happens because a) the sublanes are so varied that no
    # one would want to see a big list containing everything, and b)
    # the sublanes are exhaustive of the Lane's content, so there's
    # nothing new to be seen by going into that big list.
    include_self_in_grouped_feed: Mapped[bool] = Column(
        Boolean, default=True, nullable=False
    )

    # Only a visible lane will show up in the user interface.  The
    # admin interface can see all the lanes, visible or not.
    _visible: Mapped[bool] = Column("visible", Boolean, default=True, nullable=False)

    __table_args__ = (UniqueConstraint("parent_id", "display_name"),)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # We add this property to the class, so that we can disable the sqlalchemy
        # listener that calls site_configuration_has_changed. Calling this repeatedly
        # when updating lanes can cause performance issues. The plan is for this to
        # be a temporary fix while we replace the need for the site_configuration_has_changed
        # and listeners at all.
        # TODO: we should remove this, once we remove the site_configuration_has_changed listeners
        self._suppress_before_flush_listeners = False

    def get_library(self, _db):
        """For compatibility with WorkList.get_library()."""
        return self.library

    @property
    def collection_ids(self):
        return [x.id for x in self.library.active_collections]

    @property
    def children(self):
        return self.sublanes

    @property
    def visible_children(self):
        children = [lane for lane in self.sublanes if lane.visible]
        return sorted(children, key=lambda x: (x.priority, x.display_name or ""))

    @property
    def parentage(self):
        """Yield the parent, grandparent, etc. of this Lane.

        The Lane may be inside one or more non-Lane WorkLists, but those
        WorkLists are not counted in the parentage.
        """
        if not self.parent:
            return
        parent = self.parent
        if Session.object_session(parent) is None:
            # This lane's parent was disconnected from its database session,
            # probably when an app server started up.
            # Reattach it to the database session used by this lane.
            parent = Session.object_session(self).merge(parent)

        yield parent
        seen = {self, parent}
        for grandparent in parent.parentage:
            if grandparent in seen:
                raise ValueError("Lane parentage loop detected")
            seen.add(grandparent)
            yield grandparent

    def is_self_or_descendant(self, ancestor):
        """Is this WorkList the given WorkList or one of its descendants?

        :param ancestor: A WorkList.
        :return: A boolean.
        """
        if super().is_self_or_descendant(ancestor):
            return True

        # A TopLevelWorkList won't show up in a Lane's parentage,
        # because it's not a Lane, but if they share the same library
        # it can be presumed to be the lane's ultimate ancestor.
        if (
            isinstance(ancestor, TopLevelWorkList)
            and self.library_id == ancestor.library_id
        ):
            return True
        return False

    @property
    def depth(self):
        """How deep is this lane in this site's hierarchy?
        i.e. how many times do we have to follow .parent before we get None?
        """
        return len(list(self.parentage))

    @property
    def entrypoints(self):
        """Lanes cannot currently have EntryPoints."""
        return []

    @hybrid_property
    def visible(self):
        return self._visible and (not self.parent or self.parent.visible)

    @visible.setter
    def visible(self, value):
        self._visible = value

    @property
    def url_name(self):
        """Return the name of this lane to be used in URLs.

        Since most aspects of the lane can change through administrative
        action, we use the internal database ID of the lane in URLs.
        """
        return self.id

    @hybrid_property
    def audiences(self) -> list[str]:
        return self._audiences or []

    @audiences.setter
    def audiences(self, value: list[str] | str) -> None:
        """The `audiences` field cannot be set to a value that
        contradicts the current value to the `target_age` field.
        """
        if self._audiences and self._target_age and value != self._audiences:
            raise ValueError(
                "Cannot modify Lane.audiences when Lane.target_age is set!"
            )
        if isinstance(value, (bytes, str)):
            value = [value]
        self._audiences = value

    @hybrid_property
    def target_age(self):
        return self._target_age

    @target_age.setter
    def target_age(self, value):
        """Setting .target_age will lock .audiences to appropriate values.

        If you set target_age to 16-18, you're saying that the audiences
        are [Young Adult, Adult].

        If you set target_age 12-15, you're saying that the audiences are
        [Young Adult, Children].

        If you set target age 0-2, you're saying that the audiences are
        [Children].

        In no case is the "Adults Only" audience allowed, since target
        age only makes sense in lanes intended for minors.
        """
        if value is None:
            self._target_age = None
            return
        audiences = []
        if isinstance(value, int):
            value = (value, value)
        if isinstance(value, tuple):
            value = tuple_to_numericrange(value)
        if value.lower >= Classifier.ADULT_AGE_CUTOFF:
            # Adults are adults and there's no point in tracking
            # precise age gradations for them.
            value = tuple_to_numericrange((Classifier.ADULT_AGE_CUTOFF, value.upper))
        if value.upper >= Classifier.ADULT_AGE_CUTOFF:
            value = tuple_to_numericrange((value.lower, Classifier.ADULT_AGE_CUTOFF))
        self._target_age = value

        if value.upper >= Classifier.ADULT_AGE_CUTOFF:
            audiences.append(Classifier.AUDIENCE_ADULT)
        if value.lower < Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.append(Classifier.AUDIENCE_CHILDREN)
        if value.upper >= Classifier.YOUNG_ADULT_AGE_CUTOFF:
            audiences.append(Classifier.AUDIENCE_YOUNG_ADULT)
        self._audiences = audiences

    @hybrid_property
    def list_datasource(self):
        return self._list_datasource

    @list_datasource.setter
    def list_datasource(self, value):
        """Setting .list_datasource to a non-null value wipes out any specific
        CustomLists previously associated with this Lane.
        """
        if value:
            self.customlists = []
            if hasattr(self, "_customlist_ids"):
                # The next time someone asks for .customlist_ids,
                # the list will be refreshed.
                del self._customlist_ids

        # TODO: It's not clear to me why it's necessary to set these two
        # values separately.
        self._list_datasource = value
        self._list_datasource_id = value.id

    @property
    def list_datasource_id(self):
        if self._list_datasource_id:
            return self._list_datasource_id
        return None

    @property
    def uses_customlists(self):
        """Does the works() implementation for this Lane look for works on
        CustomLists?
        """
        if self.customlists or self.list_datasource:
            return True
        if (
            self.parent
            and self.inherit_parent_restrictions
            and self.parent.uses_customlists
        ):
            return True
        return False

    def update_size(self, _db, search_engine: ExternalSearchIndex):
        """Update the stored estimate of the number of Works in this Lane."""
        # Local import to avoid circular dependency between lane.py and feed/facets
        from palace.manager.feed.facets.database import DatabaseBackedFacets

        library = self.get_library(_db)

        # Do the estimate for every known entry point.
        by_entrypoint = dict()
        for entrypoint in EntryPoint.ENTRY_POINTS:
            facets = DatabaseBackedFacets(
                library,
                FacetConstants.AVAILABLE_ALL,
                order=FacetConstants.ORDER_WORK_ID,
                distributor=FacetConstants.DISTRIBUTOR_ALL,
                collection_name=FacetConstants.COLLECTION_NAME_ALL,
                entrypoint=entrypoint,
            )
            filter = self.filter(_db, facets)
            by_entrypoint[entrypoint.URI] = search_engine.count_works(filter)
        self.size_by_entrypoint = by_entrypoint
        self.size = by_entrypoint[EverythingEntryPoint.URI]

    @property
    def genre_ids(self):
        """Find the database ID of every Genre such that a Work classified in
        that Genre should be in this Lane.

        :return: A list of genre IDs, or None if this Lane does not
            consider genres at all.
        """
        if not hasattr(self, "_genre_ids"):
            self._genre_ids = self._gather_genre_ids()
        return self._genre_ids

    def _gather_genre_ids(self):
        """Method that does the work of `genre_ids`."""
        if not self.lane_genres:
            return None

        included_ids = set()
        excluded_ids = set()
        for lanegenre in self.lane_genres:
            genre = lanegenre.genre
            if lanegenre.inclusive:
                bucket = included_ids
            else:
                bucket = excluded_ids
            if (
                self.fiction != None
                and genre.default_fiction != None
                and self.fiction != genre.default_fiction
            ):
                logging.error(
                    "Lane %s has a genre %s that does not match its fiction restriction.",
                    (self.full_identifier, genre.name),
                )
            bucket.add(genre.id)
            if lanegenre.recursive:
                for subgenre in genre.subgenres:
                    bucket.add(subgenre.id)
        if not included_ids:
            # No genres have been explicitly included, so this lane
            # includes all genres that aren't excluded.
            _db = Session.object_session(self)
            included_ids = {genre.id for genre in _db.query(Genre)}
        genre_ids = included_ids - excluded_ids
        if not genre_ids:
            # This can happen if you create a lane where 'Epic
            # Fantasy' is included but 'Fantasy' and its subgenres are
            # excluded.
            logging.error(
                "Lane %s has a self-negating set of genre IDs.", self.full_identifier
            )
        return genre_ids

    @property
    def customlist_ids(self):
        """Find the database ID of every CustomList such that a Work filed
        in that List should be in this Lane.

        :return: A list of CustomList IDs, possibly empty.
        """
        if not hasattr(self, "_customlist_ids"):
            self._customlist_ids = self._gather_customlist_ids()
        return self._customlist_ids

    def _gather_customlist_ids(self):
        """Method that does the work of `customlist_ids`."""
        if self.list_datasource:
            # Find the ID of every CustomList from a certain
            # DataSource.
            _db = Session.object_session(self)
            query = select(CustomList.id).where(
                CustomList.data_source_id == self.list_datasource.id
            )
            ids = [x[0] for x in _db.execute(query)]
        else:
            # Find the IDs of some specific CustomLists.
            ids = [x.id for x in self.customlists]
        if len(ids) == 0:
            if self.list_datasource:
                # We are restricted to all lists from a given data
                # source, and there are no such lists, so we want to
                # exclude everything.
                return []
            else:
                # There is no custom list restriction at all.
                return None
        return ids

    @classmethod
    def affected_by_customlist(self, customlist):
        """Find all Lanes whose membership is partially derived
        from the membership of the given CustomList.
        """
        _db = Session.object_session(customlist)

        # Either the data source must match, or there must be a specific link
        # between the Lane and the CustomList.
        data_source_matches = Lane._list_datasource_id == customlist.data_source_id
        specific_link = CustomList.id == customlist.id

        return (
            _db.query(Lane)
            .outerjoin(Lane.customlists)
            .filter(or_(data_source_matches, specific_link))
        )

    def add_genre(self, genre, inclusive=True, recursive=True):
        """Create a new LaneGenre for the given genre and
        associate it with this Lane.

        Mainly used in tests.
        """
        _db = Session.object_session(self)
        if isinstance(genre, (bytes, str)):
            genre, ignore = Genre.lookup(_db, genre)
        lanegenre, is_new = get_one_or_create(_db, LaneGenre, lane=self, genre=genre)
        lanegenre.inclusive = inclusive
        lanegenre.recursive = recursive
        self._genre_ids = self._gather_genre_ids()
        return lanegenre, is_new

    @property
    def search_target(self):
        """Obtain the WorkList that should be searched when someone
        initiates a search from this Lane."""

        # See if this Lane is the root lane for a patron type, or has an
        # ancestor that's the root lane for a patron type. If so, search
        # that Lane.
        if self.root_for_patron_type:
            return self

        for parent in self.parentage:
            if parent.root_for_patron_type:
                return parent

        # Otherwise, we want to use the lane's languages, media, and
        # juvenile audiences in search.
        languages = self.languages
        media = self.media
        audiences = None
        if (
            Classifier.AUDIENCE_YOUNG_ADULT in self.audiences
            or Classifier.AUDIENCE_CHILDREN in self.audiences
        ):
            audiences = self.audiences

        # If there are too many languages or audiences, the description
        # could get too long to be useful, so we'll leave them out.
        # Media isn't part of the description yet.

        display_name_parts = []
        if languages and len(languages) <= 2:
            display_name_parts.append(LanguageCodes.name_for_languageset(languages))

        if audiences:
            if len(audiences) <= 2:
                display_name_parts.append(" and ".join(audiences))

        display_name = " ".join(display_name_parts)

        wl = WorkList()
        wl.initialize(
            self.library,
            display_name=display_name,
            languages=languages,
            media=media,
            audiences=audiences,
        )
        return wl

    def _size_for_facets(self, facets):
        """How big is this lane under the given `Facets` object?

        :param facets: A Facets object.
        :return: An int.
        """
        # Default to the total size of the lane.
        size = self.size

        entrypoint_name = EverythingEntryPoint.URI
        if facets and facets.entrypoint:
            entrypoint_name = facets.entrypoint.URI

        if self.size_by_entrypoint and entrypoint_name in self.size_by_entrypoint:
            size = self.size_by_entrypoint[entrypoint_name]
        return size

    @inject
    def groups(
        self,
        _db,
        include_sublanes=True,
        pagination=None,
        facets=None,
        *,
        search_engine: ExternalSearchIndex = Provide["search.index"],
        debug=False,
    ):
        """Return a list of (Work, Lane) 2-tuples
        describing a sequence of featured items for this lane and
        (optionally) its children.

        :param pagination: A Pagination object which may affect how many
            works each child of this WorkList may contribute.
        :param facets: A FeaturedFacets object.
        """
        if self.include_self_in_grouped_feed:
            relevant_lanes = [self]
        else:
            relevant_lanes = []
        if include_sublanes:
            # The child lanes go first.
            relevant_lanes = list(self.visible_children) + relevant_lanes

        # We can use a single query to build the featured feeds for
        # this lane, as well as any of its sublanes that inherit this
        # lane's restrictions. Lanes that don't inherit this lane's
        # restrictions will need to be handled in a separate call to
        # groups().
        queryable_lanes = [
            x for x in relevant_lanes if x == self or x.inherit_parent_restrictions
        ]
        return self._groups_for_lanes(
            _db,
            relevant_lanes,
            queryable_lanes,
            pagination=pagination,
            facets=facets,
            debug=debug,
        )

    def search(self, _db, query_string, search_client, pagination=None, facets=None):
        """Find works in this lane that also match a search query.

        :param _db: A database connection.
        :param query_string: Search for this string.
        :param search_client: An ExternalSearchIndex object.
        :param pagination: A Pagination object.
        :param facets: A faceting object, probably a SearchFacets.
        """
        search_target = self.search_target

        if search_target == self:
            # The actual implementation happens in WorkList.
            m = super().search
        else:
            # Searches in this Lane actually go against some other WorkList.
            # Tell that object to run the search.
            m = search_target.search

        return m(_db, query_string, search_client, pagination, facets=facets)

    def explain(self):
        """Create a series of human-readable strings to explain a lane's settings."""
        lines = []
        lines.append("ID: %s" % self.id)
        lines.append("Library: %s" % self.library.short_name)
        if self.parent:
            lines.append(f"Parent ID: {self.parent.id} ({self.parent.display_name})")
        lines.append("Priority: %s" % self.priority)
        lines.append("Display name: %s" % self.display_name)
        return lines


lanes_customlists = Table(
    "lanes_customlists",
    Base.metadata,
    Column("lane_id", Integer, ForeignKey("lanes.id"), index=True, nullable=False),
    Column(
        "customlist_id",
        Integer,
        ForeignKey("customlists.id"),
        index=True,
        nullable=False,
    ),
    UniqueConstraint("lane_id", "customlist_id"),
)
