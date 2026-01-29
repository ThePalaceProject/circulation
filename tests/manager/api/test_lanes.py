from collections import Counter
from unittest.mock import patch

from palace.manager.api.lanes import (
    _lane_configuration_from_collection_sizes,
    create_default_lanes,
    create_lane_for_small_collection,
    create_lane_for_tiny_collection,
    create_lanes_for_large_collection,
    create_world_languages_lane,
)
from palace.manager.core.classifier import Classifier
from palace.manager.integration.goals import Goals
from palace.manager.integration.metadata.nyt import (
    NYTBestSellerAPI,
    NytBestSellerApiSettings,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import (
    Lane,
)
from palace.manager.sqlalchemy.util import numericrange_to_tuple
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


class TestLaneCreation:
    NONEXISTENT_ALPHA3 = "nqq"

    def test_create_lanes_for_large_collection(self, db: DatabaseTransactionFixture):
        languages = ["eng", "spa"]
        create_lanes_for_large_collection(db.session, db.default_library(), languages)
        lanes = (
            db.session.query(Lane)
            .filter(Lane.parent_id == None)
            .order_by(Lane.priority)
            .all()
        )

        # We have five top-level lanes.
        assert 5 == len(lanes)
        assert [
            "Fiction",
            "Nonfiction",
            "Young Adult Fiction",
            "Young Adult Nonfiction",
            "Children and Middle Grade",
        ] == [x.display_name for x in lanes]
        for lane in lanes:
            assert db.default_library() == lane.library
            # They all are restricted to English and Spanish.
            assert lane.languages == languages

            # They have no restrictions on media type -- that's handled
            # with entry points.
            assert None == lane.media

        assert [
            "Fiction",
            "Nonfiction",
            "Young Adult Fiction",
            "Young Adult Nonfiction",
            "Children and Middle Grade",
        ] == [x.display_name for x in lanes]

        # The Adult Fiction and Adult Nonfiction lanes reproduce the
        # genre structure found in the genre definitions.
        fiction, nonfiction = lanes[0:2]
        [sf] = [x for x in fiction.sublanes if "Science Fiction" in x.display_name]
        [periodicals] = [
            x for x in nonfiction.sublanes if "Periodicals" in x.display_name
        ]
        assert True == sf.fiction
        assert "Science Fiction" == sf.display_name
        assert "Science Fiction" in [genre.name for genre in sf.genres]

        [nonfiction_humor] = [
            x for x in nonfiction.sublanes if "Humor" in x.display_name
        ]
        assert False == nonfiction_humor.fiction

        [fiction_humor] = [x for x in fiction.sublanes if "Humor" in x.display_name]
        assert True == fiction_humor.fiction

        [space_opera] = [x for x in sf.sublanes if "Space Opera" in x.display_name]
        assert True == sf.fiction
        assert "Space Opera" == space_opera.display_name
        assert ["Space Opera"] == [genre.name for genre in space_opera.genres]

        [history] = [x for x in nonfiction.sublanes if "History" in x.display_name]
        assert False == history.fiction
        assert "History" == history.display_name
        assert "History" in [genre.name for genre in history.genres]
        [european_history] = [
            x for x in history.sublanes if "European History" in x.display_name
        ]
        assert "European History" in [genre.name for genre in european_history.genres]

        # Delete existing lanes.
        for lane in db.session.query(Lane).filter(
            Lane.library_id == db.default_library().id
        ):
            db.session.delete(lane)

        # If there's an NYT Best Sellers integration and we create the lanes again...
        db.integration_configuration(
            NYTBestSellerAPI,
            goal=Goals.METADATA_GOAL,
            settings=NytBestSellerApiSettings(password="foo"),
        )

        create_lanes_for_large_collection(db.session, db.default_library(), languages)
        lanes = (
            db.session.query(Lane)
            .filter(Lane.parent_id == None)
            .order_by(Lane.priority)
            .all()
        )

        # Now we have six top-level lanes, with best sellers at the beginning.
        assert [
            "Best Sellers",
            "Fiction",
            "Nonfiction",
            "Young Adult Fiction",
            "Young Adult Nonfiction",
            "Children and Middle Grade",
        ] == [x.display_name for x in lanes]

        # Each sublane other than best sellers also contains a best sellers lane.
        for sublane in lanes[1:]:
            best_sellers = sublane.visible_children[0]
            assert "Best Sellers" == best_sellers.display_name

        # The best sellers lane has a data source.
        nyt_data_source = DataSource.lookup(db.session, DataSource.NYT)
        assert nyt_data_source == lanes[0].list_datasource

    def test_create_world_languages_lane(self, db: DatabaseTransactionFixture):
        # If there are no small or tiny collections, calling
        # create_world_languages_lane does not create any lanes or change
        # the priority.
        new_priority = create_world_languages_lane(
            db.session, db.default_library(), [], [], priority=10
        )
        assert 10 == new_priority
        assert [] == db.session.query(Lane).all()

        # If there are lanes to be created, create_world_languages_lane
        # creates them.
        new_priority = create_world_languages_lane(
            db.session, db.default_library(), ["eng"], [["spa", "fre"]], priority=10
        )

        # priority has been incremented to make room for the newly
        # created lane.
        assert 11 == new_priority

        # One new top-level lane has been created. It contains books
        # from all three languages mentioned in its children.
        top_level = db.session.query(Lane).filter(Lane.parent == None).one()
        assert "World Languages" == top_level.display_name
        assert {"spa", "fre", "eng"} == set(top_level.languages)

        # It has two children -- one for the small English collection and
        # one for the tiny Spanish/French collection.,
        small, tiny = top_level.visible_children
        assert "English" == small.display_name
        assert ["eng"] == small.languages

        assert "espa\xf1ol/fran\xe7ais" == tiny.display_name
        assert {"spa", "fre"} == set(tiny.languages)

        # The tiny collection has no sublanes, but the small one has
        # three.  These lanes are tested in more detail in
        # test_create_lane_for_small_collection.
        fiction, nonfiction, children = small.sublanes
        assert [] == tiny.sublanes
        assert "Fiction" == fiction.display_name
        assert "Nonfiction" == nonfiction.display_name
        assert "Children & Young Adult" == children.display_name

    def test_create_lane_for_small_collection(self, db: DatabaseTransactionFixture):
        languages = ["eng", "spa", "chi"]
        create_lane_for_small_collection(
            db.session, db.default_library(), None, languages
        )
        [lane] = db.session.query(Lane).filter(Lane.parent_id == None).all()

        assert "English/español/Chinese" == lane.display_name
        sublanes = lane.visible_children
        assert ["Fiction", "Nonfiction", "Children & Young Adult"] == [
            x.display_name for x in sublanes
        ]
        for x in sublanes:
            assert languages == x.languages
            assert [Edition.BOOK_MEDIUM] == x.media

        assert [
            {"All Ages", "Adults Only", "Adult"},
            {"All Ages", "Adults Only", "Adult"},
            {"Young Adult", "Children"},
        ] == [set(x.audiences) for x in sublanes]
        assert [True, False, None] == [x.fiction for x in sublanes]

        # If any language codes do not map to a name, don't create any lanes.
        languages = ["eng", self.NONEXISTENT_ALPHA3, "chi"]
        parent = db.lane()
        priority = create_lane_for_small_collection(
            db.session, db.default_library(), parent, languages, priority=2
        )
        lane = db.session.query(Lane).filter(Lane.parent == parent)
        assert priority == 0
        assert lane.count() == 0

    def test_lane_for_tiny_collection(self, db: DatabaseTransactionFixture):
        parent = db.lane()
        new_priority = create_lane_for_tiny_collection(
            db.session, db.default_library(), parent, "ger", priority=3
        )
        assert 4 == new_priority
        lane = db.session.query(Lane).filter(Lane.parent == parent).one()
        assert [Edition.BOOK_MEDIUM] == lane.media
        assert parent == lane.parent
        assert ["ger"] == lane.languages
        assert "Deutsch" == lane.display_name
        assert [] == lane.children

        # No lane should be created when the language has no name.
        new_parent = db.lane()
        new_priority = create_lane_for_tiny_collection(
            db.session,
            db.default_library(),
            new_parent,
            ["spa", self.NONEXISTENT_ALPHA3, "eng"],
            priority=3,
        )
        assert 0 == new_priority
        lane = db.session.query(Lane).filter(Lane.parent == new_parent)
        assert lane.count() == 0

    def test_create_default_lanes(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        settings = library_fixture.mock_settings()
        settings.large_collection_languages = ["eng"]
        settings.small_collection_languages = ["spa", "chi"]
        settings.tiny_collection_languages = ["ger", "fre", "ita"]
        library = library_fixture.library(settings=settings)

        create_default_lanes(db.session, library)
        lanes = (
            db.session.query(Lane)
            .filter(Lane.library == library)
            .filter(Lane.parent_id == None)
            .all()
        )

        # We have five top-level lanes for the large collection,
        # a top-level lane for each small collection, and a lane
        # for everything left over.
        assert {
            "Fiction",
            "Nonfiction",
            "Young Adult Fiction",
            "Young Adult Nonfiction",
            "Children and Middle Grade",
            "World Languages",
        } == {x.display_name for x in lanes}

        [english_fiction_lane] = [x for x in lanes if x.display_name == "Fiction"]
        assert 0 == english_fiction_lane.priority
        [world] = [x for x in lanes if x.display_name == "World Languages"]
        assert 5 == world.priority

        # ensure the target age is appropriately set for the children and middle grade lane
        [children_and_middle_grade_lane] = [
            x for x in lanes if x.display_name == "Children and Middle Grade"
        ]
        assert numericrange_to_tuple(children_and_middle_grade_lane.target_age) == (
            0,
            13,
        )
        # and that the audience is set to children
        audiences = children_and_middle_grade_lane.audiences
        assert 1 == len(audiences)
        assert Classifier.AUDIENCE_CHILDREN == audiences[0]

    def test_create_default_when_more_than_one_large_language_is_configured(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        settings = library_fixture.mock_settings()
        settings.large_collection_languages = ["eng", "fre"]
        library = library_fixture.library(settings=settings)

        session = db.session
        create_default_lanes(session, library)
        lanes = (
            session.query(Lane)
            .filter(Lane.library == library)
            .filter(Lane.parent_id == None)
            .all()
        )

        # We have five top-level lanes for the large collection,
        # and no world languages lane
        assert {
            "Fiction",
            "Nonfiction",
            "Young Adult Fiction",
            "Young Adult Nonfiction",
            "Children and Middle Grade",
        } == {x.display_name for x in lanes}

    def test_create_default_when_more_than_one_large_language_is_returned_by_estimation(
        self, db: DatabaseTransactionFixture
    ):
        library = db.default_library()
        session = db.session
        with patch(
            "palace.manager.api.lanes._lane_configuration_from_collection_sizes"
        ) as mock_lane_config:
            mock_lane_config.return_value = (["eng", "fre"], [], [])
            create_default_lanes(session, library)
            lanes = (
                session.query(Lane)
                .filter(Lane.library == library)
                .filter(Lane.parent_id == None)
                .all()
            )

            # We have five top-level lanes for the large collection,
            # and no world languages lane
            assert {
                "Fiction",
                "Nonfiction",
                "Young Adult Fiction",
                "Young Adult Nonfiction",
                "Children and Middle Grade",
            } == {x.display_name for x in lanes}

    def test_lane_configuration_from_collection_sizes(self):
        # If the library has no holdings, we assume it has a large English
        # collection.
        m = _lane_configuration_from_collection_sizes
        assert (["eng"], [], []) == m(None)
        assert (["eng"], [], []) == m(Counter())

        # Otherwise, the language with the largest collection, and all
        # languages more than 10% as large, go into `large`.  All
        # languages with collections more than 1% as large as the
        # largest collection go into `small`. All languages with
        # smaller collections go into `tiny`.
        base = 10000
        holdings = Counter(
            large1=base,
            large2=base * 0.1001,
            small1=base * 0.1,
            small2=base * 0.01001,
            tiny=base * 0.01,
        )
        large, small, tiny = m(holdings)
        assert {"large1", "large2"} == set(large)
        assert {"small1", "small2"} == set(small)
        assert ["tiny"] == tiny
