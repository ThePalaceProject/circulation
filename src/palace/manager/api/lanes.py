import logging

import palace.manager.core.classifier as genres
from palace.manager.api.config import Configuration
from palace.manager.core import classifier
from palace.manager.core.classifier import (
    Classifier,
    GenreData,
    fiction_genres,
    nonfiction_genres,
)
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.feed.worklist.base import WorkList
from palace.manager.integration.metadata.nyt import NYTBestSellerAPI
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import (
    Lane,
)
from palace.manager.sqlalchemy.util import create
from palace.manager.util.languages import LanguageCodes

log = logging.getLogger(__name__)


def load_lanes(_db, library, collection_ids):
    """Return a WorkList that reflects the current lane structure of the
    Library.

    If no top-level visible lanes are configured, the WorkList will be
    configured to show every book in the collection.

    If a single top-level Lane is configured, it will returned as the
    WorkList.

    Otherwise, a WorkList containing the visible top-level lanes is
    returned.
    """
    top_level = WorkList.top_level_for_library(
        _db, library, collection_ids=collection_ids
    )

    # It's likely this WorkList will be used across sessions, so
    # expunge any data model objects from the database session.
    #
    # TODO: This is the cause of a lot of problems in the cached OPDS
    # feed generator. There, these Lanes are used in a normal database
    # session and we end up needing hacks to merge them back into the
    # session.
    if isinstance(top_level, Lane):
        to_expunge = [top_level]
    else:
        to_expunge = [x for x in top_level.children if isinstance(x, Lane)]
    list(map(_db.expunge, to_expunge))
    return top_level


def _lane_configuration_from_collection_sizes(estimates):
    """Sort a library's collections into 'large', 'small', and 'tiny'
    subcollections based on language.

    :param estimates: A Counter.

    :return: A 3-tuple (large, small, tiny). 'large' will contain the
    collection with the largest language, and any languages with a
    collection more than 10% the size of the largest
    collection. 'small' will contain any languages with a collection
    more than 1% the size of the largest collection, and 'tiny' will
    contain all other languages represented in `estimates`.
    """
    if not estimates:
        # There are no holdings. Assume we have a large English
        # collection and nothing else.
        return ["eng"], [], []

    large = []
    small = []
    tiny = []

    [(ignore, largest)] = estimates.most_common(1)
    for language, count in estimates.most_common():
        if count > largest * 0.1:
            large.append(language)
        elif count > largest * 0.01:
            small.append(language)
        else:
            tiny.append(language)
    return large, small, tiny


def create_default_lanes(_db, library):
    """Reset the lanes for the given library to the default.

    This method will create a set of default lanes  for the first
    major language specified in the UI or if no language is specified
    then the most represented language in the catalogue.  If more than
    one major language is specified, all but the first will be ignored
    in terms of default lane creation. In other words, don't specify
    multiple top level languages.

    If there are any small- or tiny-collection languages, the database
    will also have a top-level lane called 'World Languages'. The
    'World Languages' lane will have a sublane for every small- and
    tiny-collection languages. The small-collection languages will
    have "Adult Fiction", "Adult Nonfiction", and "Children/YA"
    sublanes; the tiny-collection languages will not have any sublanes.

    If run on a Library that already has Lane configuration, this can
    be an extremely destructive method. All new Lanes will be visible
    and all Lanes based on CustomLists (but not the CustomLists
    themselves) will be destroyed.

    """
    # Delete existing lanes.
    for lane in _db.query(Lane).filter(Lane.library_id == library.id):
        _db.delete(lane)

    top_level_lanes = []

    # Hopefully this library is configured with explicit guidance as
    # to how the languages should be set up.
    large = Configuration.large_collection_languages(library) or []
    small = Configuration.small_collection_languages(library) or []
    tiny = Configuration.tiny_collection_languages(library) or []

    # If there are no language configuration settings, we can estimate
    # the current collection size to determine the lanes.
    if not large and not small and not tiny:
        estimates = library.estimated_holdings_by_language()
        large, small, tiny = _lane_configuration_from_collection_sizes(estimates)
    priority = 0

    if large and len(large) > 0:
        language = large[0]
        priority = create_lanes_for_large_collection(
            _db, library, language, priority=priority
        )

    create_world_languages_lane(_db, library, small, tiny, priority)


def lane_from_genres(
    _db,
    library,
    genres,
    display_name=None,
    exclude_genres=None,
    priority=0,
    audiences=None,
    **extra_args
):
    """Turn genre info into a Lane object."""

    genre_lane_instructions = {
        "Dystopian SF": dict(display_name="Dystopian"),
        "Erotica": dict(audiences=[Classifier.AUDIENCE_ADULTS_ONLY]),
        "Humorous Fiction": dict(display_name="Humor"),
        "Media Tie-in SF": dict(display_name="Movie and TV Novelizations"),
        "Suspense/Thriller": dict(display_name="Thriller"),
        "Humorous Nonfiction": dict(display_name="Humor"),
        "Political Science": dict(display_name="Politics & Current Events"),
        "Periodicals": dict(visible=False),
    }

    # Create sublanes first.
    sublanes = []
    for genre in genres:
        if isinstance(genre, dict):
            sublane_priority = 0
            for subgenre in genre.get("subgenres", []):
                sublanes.append(
                    lane_from_genres(
                        _db,
                        library,
                        [subgenre],
                        priority=sublane_priority,
                        **extra_args
                    )
                )
                sublane_priority += 1

    # Now that we have sublanes we don't care about subgenres anymore.
    genres = [
        (
            genre.get("name")
            if isinstance(genre, dict)
            else genre.name if isinstance(genre, GenreData) else genre
        )
        for genre in genres
    ]

    exclude_genres = [
        (
            genre.get("name")
            if isinstance(genre, dict)
            else genre.name if isinstance(genre, GenreData) else genre
        )
        for genre in exclude_genres or []
    ]

    fiction = None
    visible = True
    if len(genres) == 1:
        if classifier.genres.get(genres[0]):
            genredata = classifier.genres[genres[0]]
        else:
            genredata = GenreData(genres[0], False)
        fiction = genredata.is_fiction

        if genres[0] in list(genre_lane_instructions.keys()):
            instructions = genre_lane_instructions[genres[0]]
            if not display_name and "display_name" in instructions:
                display_name = instructions.get("display_name")
            if "audiences" in instructions:
                audiences = instructions.get("audiences")
            if "visible" in instructions:
                visible = instructions.get("visible")

    if not display_name:
        display_name = ", ".join(sorted(genres))

    lane, ignore = create(
        _db,
        Lane,
        library_id=library.id,
        display_name=display_name,
        fiction=fiction,
        audiences=audiences,
        sublanes=sublanes,
        priority=priority,
        **extra_args
    )
    lane.visible = visible
    for genre in genres:
        lane.add_genre(genre)
    for genre in exclude_genres:
        lane.add_genre(genre, inclusive=False)
    return lane


def create_lanes_for_large_collection(_db, library, languages, priority=0):
    """Ensure that the lanes appropriate to a large collection are all
    present.

    This means:

    * A "%(language)s Adult Fiction" lane containing sublanes for each fiction
        genre.
    * A "%(language)s Adult Nonfiction" lane containing sublanes for
        each nonfiction genre.
    * A "%(language)s YA Fiction" lane containing sublanes for the
        most popular YA fiction genres.
    * A "%(language)s YA Nonfiction" lane containing sublanes for the
        most popular YA fiction genres.
    * A "%(language)s Children and Middle Grade" lane containing
        sublanes for childrens' books at different age levels.

    :param library: Newly created lanes will be associated with this
        library.
    :param languages: Newly created lanes will contain only books
        in these languages.
    :return: A list of top-level Lane objects.

    TODO: If there are multiple large collections, their top-level lanes do
    not have distinct display names.
    """
    if isinstance(languages, str):
        languages = [languages]

    ADULT = Classifier.AUDIENCES_ADULT
    YA = [Classifier.AUDIENCE_YOUNG_ADULT]
    CHILDREN = [Classifier.AUDIENCE_CHILDREN]

    common_args = dict(languages=languages, media=None)
    adult_common_args = dict(common_args)
    adult_common_args["audiences"] = ADULT

    nyt_data_source = DataSource.lookup(_db, DataSource.NYT)
    try:
        NYTBestSellerAPI.from_config(_db)
        include_best_sellers = True
    except CannotLoadConfiguration:
        # No NYT Best Seller integration is configured.
        include_best_sellers = False

    sublanes = []
    if include_best_sellers:
        best_sellers, ignore = create(
            _db,
            Lane,
            library=library,
            display_name="Best Sellers",
            priority=priority,
            **common_args
        )
        priority += 1
        best_sellers.list_datasource = nyt_data_source
        sublanes.append(best_sellers)

    adult_fiction_sublanes = []
    adult_fiction_priority = 0
    if include_best_sellers:
        adult_fiction_best_sellers, ignore = create(
            _db,
            Lane,
            library=library,
            display_name="Best Sellers",
            fiction=True,
            priority=adult_fiction_priority,
            **adult_common_args
        )
        adult_fiction_priority += 1
        adult_fiction_best_sellers.list_datasource = nyt_data_source
        adult_fiction_sublanes.append(adult_fiction_best_sellers)

    for genre in fiction_genres:
        if isinstance(genre, str):
            genre_name = genre
        else:
            genre_name = genre.get("name")
        genre_lane = lane_from_genres(
            _db, library, [genre], priority=adult_fiction_priority, **adult_common_args
        )
        adult_fiction_priority += 1
        adult_fiction_sublanes.append(genre_lane)

    adult_fiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Fiction",
        genres=[],
        sublanes=adult_fiction_sublanes,
        fiction=True,
        priority=priority,
        **adult_common_args
    )
    priority += 1
    sublanes.append(adult_fiction)

    adult_nonfiction_sublanes = []
    adult_nonfiction_priority = 0
    if include_best_sellers:
        adult_nonfiction_best_sellers, ignore = create(
            _db,
            Lane,
            library=library,
            display_name="Best Sellers",
            fiction=False,
            priority=adult_nonfiction_priority,
            **adult_common_args
        )
        adult_nonfiction_priority += 1
        adult_nonfiction_best_sellers.list_datasource = nyt_data_source
        adult_nonfiction_sublanes.append(adult_nonfiction_best_sellers)

    for genre in nonfiction_genres:
        # "Life Strategies" is a YA-specific genre that should not be
        # included in the Adult Nonfiction lane.
        if genre != genres.Life_Strategies:
            if isinstance(genre, str):
                genre_name = genre
            else:
                genre_name = genre.get("name")
            genre_lane = lane_from_genres(
                _db,
                library,
                [genre],
                priority=adult_nonfiction_priority,
                **adult_common_args
            )
            adult_nonfiction_priority += 1
            adult_nonfiction_sublanes.append(genre_lane)

    adult_nonfiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Nonfiction",
        genres=[],
        sublanes=adult_nonfiction_sublanes,
        fiction=False,
        priority=priority,
        **adult_common_args
    )
    priority += 1
    sublanes.append(adult_nonfiction)

    ya_common_args = dict(common_args)
    ya_common_args["audiences"] = YA

    ya_fiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Young Adult Fiction",
        genres=[],
        fiction=True,
        sublanes=[],
        priority=priority,
        **ya_common_args
    )
    priority += 1
    sublanes.append(ya_fiction)

    ya_fiction_priority = 0
    if include_best_sellers:
        ya_fiction_best_sellers, ignore = create(
            _db,
            Lane,
            library=library,
            display_name="Best Sellers",
            fiction=True,
            priority=ya_fiction_priority,
            **ya_common_args
        )
        ya_fiction_priority += 1
        ya_fiction_best_sellers.list_datasource = nyt_data_source
        ya_fiction.sublanes.append(ya_fiction_best_sellers)

    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Dystopian_SF],
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Fantasy],
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Comics_Graphic_Novels],
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Literary_Fiction],
            display_name="Contemporary Fiction",
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.LGBTQ_Fiction],
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Suspense_Thriller, genres.Mystery],
            display_name="Mystery & Thriller",
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Romance],
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Science_Fiction],
            exclude_genres=[genres.Dystopian_SF, genres.Steampunk],
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1
    ya_fiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Steampunk],
            priority=ya_fiction_priority,
            **ya_common_args
        )
    )
    ya_fiction_priority += 1

    ya_nonfiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Young Adult Nonfiction",
        genres=[],
        fiction=False,
        sublanes=[],
        priority=priority,
        **ya_common_args
    )
    priority += 1
    sublanes.append(ya_nonfiction)

    ya_nonfiction_priority = 0
    if include_best_sellers:
        ya_nonfiction_best_sellers, ignore = create(
            _db,
            Lane,
            library=library,
            display_name="Best Sellers",
            fiction=False,
            priority=ya_nonfiction_priority,
            **ya_common_args
        )
        ya_nonfiction_priority += 1
        ya_nonfiction_best_sellers.list_datasource = nyt_data_source
        ya_nonfiction.sublanes.append(ya_nonfiction_best_sellers)

    ya_nonfiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Biography_Memoir],
            display_name="Biography",
            priority=ya_nonfiction_priority,
            **ya_common_args
        )
    )
    ya_nonfiction_priority += 1
    ya_nonfiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.History, genres.Social_Sciences],
            display_name="History & Sociology",
            priority=ya_nonfiction_priority,
            **ya_common_args
        )
    )
    ya_nonfiction_priority += 1
    ya_nonfiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Life_Strategies],
            priority=ya_nonfiction_priority,
            **ya_common_args
        )
    )
    ya_nonfiction_priority += 1
    ya_nonfiction.sublanes.append(
        lane_from_genres(
            _db,
            library,
            [genres.Religion_Spirituality],
            priority=ya_nonfiction_priority,
            **ya_common_args
        )
    )
    ya_nonfiction_priority += 1

    children_common_args = dict(common_args)
    children_common_args["target_age"] = Classifier.range_tuple(
        0, Classifier.YOUNG_ADULT_AGE_CUTOFF - 1
    )

    children, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Children and Middle Grade",
        genres=[],
        fiction=None,
        sublanes=[],
        priority=priority,
        **children_common_args
    )
    priority += 1
    sublanes.append(children)

    children_priority = 0
    if include_best_sellers:
        children_best_sellers, ignore = create(
            _db,
            Lane,
            library=library,
            display_name="Best Sellers",
            priority=children_priority,
            **children_common_args
        )
        children_priority += 1
        children_best_sellers.list_datasource = nyt_data_source
        children.sublanes.append(children_best_sellers)

    picture_books, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Picture Books",
        target_age=(0, 4),
        genres=[],
        fiction=None,
        priority=children_priority,
        languages=languages,
    )
    children_priority += 1
    children.sublanes.append(picture_books)

    early_readers, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Early Readers",
        target_age=(5, 8),
        genres=[],
        fiction=None,
        priority=children_priority,
        languages=languages,
    )
    children_priority += 1
    children.sublanes.append(early_readers)

    chapter_books, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Chapter Books",
        target_age=(9, 12),
        genres=[],
        fiction=None,
        priority=children_priority,
        languages=languages,
    )
    children_priority += 1
    children.sublanes.append(chapter_books)

    children_poetry, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Poetry Books",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_poetry.add_genre(genres.Poetry.name)
    children.sublanes.append(children_poetry)

    children_folklore, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Folklore",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_folklore.add_genre(genres.Folklore.name)
    children.sublanes.append(children_folklore)

    children_fantasy, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Fantasy",
        fiction=True,
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_fantasy.add_genre(genres.Fantasy.name)
    children.sublanes.append(children_fantasy)

    children_sf, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Science Fiction",
        fiction=True,
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_sf.add_genre(genres.Science_Fiction.name)
    children.sublanes.append(children_sf)

    realistic_fiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Realistic Fiction",
        fiction=True,
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    realistic_fiction.add_genre(genres.Literary_Fiction.name)
    children.sublanes.append(realistic_fiction)

    children_graphic_novels, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Comics & Graphic Novels",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_graphic_novels.add_genre(genres.Comics_Graphic_Novels.name)
    children.sublanes.append(children_graphic_novels)

    children_biography, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Biography",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_biography.add_genre(genres.Biography_Memoir.name)
    children.sublanes.append(children_biography)

    children_historical_fiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Historical Fiction",
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    children_historical_fiction.add_genre(genres.Historical_Fiction.name)
    children.sublanes.append(children_historical_fiction)

    informational, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Informational Books",
        fiction=False,
        genres=[],
        priority=children_priority,
        **children_common_args
    )
    children_priority += 1
    informational.add_genre(genres.Biography_Memoir.name, inclusive=False)
    children.sublanes.append(informational)

    return priority


def create_world_languages_lane(
    _db,
    library,
    small_languages,
    tiny_languages,
    priority=0,
):
    """Create a lane called 'World Languages' whose sublanes represent
    the non-large language collections available to this library.
    """
    if not small_languages and not tiny_languages:
        # All the languages on this system have large collections, so
        # there is no need for a 'World Languages' lane.
        return priority

    complete_language_set = set()
    for list in (small_languages, tiny_languages):
        for languageset in list:
            if isinstance(languageset, str):
                complete_language_set.add(languageset)
            else:
                complete_language_set.update(languageset)

    world_languages, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="World Languages",
        fiction=None,
        priority=priority,
        languages=complete_language_set,
        media=[Edition.BOOK_MEDIUM],
        genres=[],
    )
    priority += 1

    language_priority = 0
    for small in small_languages:
        # Create a lane (with sublanes) for each small collection.
        language_priority = create_lane_for_small_collection(
            _db, library, world_languages, small, language_priority
        )
    for tiny in tiny_languages:
        # Create a lane (no sublanes) for each tiny collection.
        language_priority = create_lane_for_tiny_collection(
            _db, library, world_languages, tiny, language_priority
        )
    return priority


def create_lane_for_small_collection(_db, library, parent, languages, priority=0):
    """Create a lane (with sublanes) for a small collection based on language,
    if the language exists in the lookup table.

    :param parent: The parent of the new lane.
    """
    if isinstance(languages, str):
        languages = [languages]

    ADULT = Classifier.AUDIENCES_ADULT
    YA_CHILDREN = [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN]

    common_args = dict(
        languages=languages,
        media=[Edition.BOOK_MEDIUM],
        genres=[],
    )

    try:
        language_identifier = LanguageCodes.name_for_languageset(languages)
    except ValueError as e:
        log.warning(
            "Could not create a lane for small collection with languages %s", languages
        )
        return 0

    sublane_priority = 0

    adult_fiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Fiction",
        fiction=True,
        audiences=ADULT,
        priority=sublane_priority,
        **common_args
    )
    sublane_priority += 1

    adult_nonfiction, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Nonfiction",
        fiction=False,
        audiences=ADULT,
        priority=sublane_priority,
        **common_args
    )
    sublane_priority += 1

    ya_children, ignore = create(
        _db,
        Lane,
        library=library,
        display_name="Children & Young Adult",
        fiction=None,
        audiences=YA_CHILDREN,
        priority=sublane_priority,
        **common_args
    )
    sublane_priority += 1

    lane, ignore = create(
        _db,
        Lane,
        library=library,
        display_name=language_identifier,
        parent=parent,
        sublanes=[adult_fiction, adult_nonfiction, ya_children],
        priority=priority,
        **common_args
    )
    priority += 1
    return priority


def create_lane_for_tiny_collection(_db, library, parent, languages, priority=0):
    """Create a single lane for a tiny collection based on language,
    if the language exists in the lookup table.

    :param parent: The parent of the new lane.
    """
    if not languages:
        return None

    if isinstance(languages, str):
        languages = [languages]

    try:
        name = LanguageCodes.name_for_languageset(languages)
    except ValueError as e:
        log.warning(
            "Could not create a lane for tiny collection with languages %s", languages
        )
        return 0

    language_lane, ignore = create(
        _db,
        Lane,
        library=library,
        display_name=name,
        parent=parent,
        genres=[],
        media=[Edition.BOOK_MEDIUM],
        fiction=None,
        priority=priority,
        languages=languages,
    )
    return priority + 1
