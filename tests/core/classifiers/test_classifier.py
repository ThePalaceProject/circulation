"""Test logic surrounding classification schemes."""

from collections import Counter

import pytest
from psycopg2.extras import NumericRange

from core import classifier
from core.classifier import (
    Classifier,
    FreeformAudienceClassifier,
    GenreData,
    Lowercased,
    WorkClassifier,
    fiction_genres,
    nonfiction_genres,
)
from core.classifier.age import (
    AgeClassifier,
    GradeLevelClassifier,
    InterestLevelClassifier,
)
from core.classifier.ddc import DeweyDecimalClassifier as DDC
from core.classifier.keyword import FASTClassifier as FAST
from core.classifier.keyword import LCSHClassifier as LCSH
from core.classifier.lcc import LCCClassifier as LCC
from core.classifier.simplified import SimplifiedGenreClassifier
from core.model import DataSource, Genre, Identifier, Subject, Work
from tests.fixtures.database import DatabaseTransactionFixture

genres = dict()
GenreData.populate(globals(), genres, fiction_genres, nonfiction_genres)


class TestLowercased:
    def test_constructor(self):
        l = Lowercased("A string")

        # A string is lowercased.
        assert "a string" == l

        # A Lowercased object is returned rather than creating a new
        # object.
        assert Lowercased(l) is l

        # A number such as a Dewey Decimal number is converted to a string.
        assert "301" == Lowercased(301)

        # A trailing period is removed.
        l = Lowercased("A string.")
        assert "a string" == l

        # The original value is still available.
        assert "A string." == l.original


class TestGenreData:
    def test_fiction_default(self):
        # In general, genres are restricted to either fiction or
        # nonfiction.
        assert True == Science_Fiction.is_fiction
        assert False == Science.is_fiction


class TestClassifier:
    def test_default_target_age_for_audience(self):
        assert (None, None) == Classifier.default_target_age_for_audience(
            Classifier.AUDIENCE_CHILDREN
        )
        assert (14, 17) == Classifier.default_target_age_for_audience(
            Classifier.AUDIENCE_YOUNG_ADULT
        )
        assert (18, None) == Classifier.default_target_age_for_audience(
            Classifier.AUDIENCE_ADULT
        )
        assert (18, None) == Classifier.default_target_age_for_audience(
            Classifier.AUDIENCE_ADULTS_ONLY
        )

    def test_default_audience_for_target_age(self):
        def aud(low, high, expect):
            assert expect == Classifier.default_audience_for_target_age((low, high))

        assert None == Classifier.default_audience_for_target_age(None)
        aud(None, None, None)
        aud(None, 17, Classifier.AUDIENCE_YOUNG_ADULT)
        aud(None, 4, Classifier.AUDIENCE_CHILDREN)
        aud(None, 44, Classifier.AUDIENCE_ADULT)
        aud(18, 44, Classifier.AUDIENCE_ADULT)
        aud(14, 14, Classifier.AUDIENCE_YOUNG_ADULT)
        aud(14, 19, Classifier.AUDIENCE_YOUNG_ADULT)
        aud(2, 14, Classifier.AUDIENCE_CHILDREN)
        aud(2, 8, Classifier.AUDIENCE_CHILDREN)

        # We treat this as YA because its target age range overlaps
        # our YA age range, and many external sources consider books
        # for twelve-year-olds to be "YA".
        aud(12, 15, Classifier.AUDIENCE_YOUNG_ADULT)

        # Whereas this is unambiguously 'Children' as far as we're concerned.
        aud(12, 13, Classifier.AUDIENCE_CHILDREN)

        # All ages for audiences that are younger than the "all ages
        # age cutoff" and older than the "adult age cutoff".
        aud(5, 18, Classifier.AUDIENCE_ALL_AGES)
        aud(5, 25, Classifier.AUDIENCE_ALL_AGES)

    def test_and_up(self):
        """Test the code that determines what "x and up" actually means."""

        def u(young, keyword):
            return Classifier.and_up(young, keyword)

        assert None == u(None, None)
        assert None == u(6, "6 years old only")
        assert 5 == u(3, "3 and up")
        assert 8 == u(6, "6+")
        assert 12 == u(8, "8+")
        assert 14 == u(10, "10+")
        assert 17 == u(12, "12 and up")
        assert 17 == u(14, "14+.")
        assert 18 == u(18, "18+")

    def test_scrub_identifier_can_override_name(self):
        """Test the ability of scrub_identifier to override the name
        of the subject for classification purposes.

        This is used e.g. in the BISACClassifier to ensure that a known BISAC
        code is always mapped to its canonical name.
        """

        class SetsNameForOneIdentifier(Classifier):
            "A Classifier that insists on a certain name for one specific identifier"

            @classmethod
            def scrub_identifier(self, identifier):
                if identifier == "A":
                    return ("A", "Use this name!")
                else:
                    return identifier

            @classmethod
            def scrub_name(self, name):
                """This verifies that the override name still gets passed
                into scrub_name.
                """
                return name.upper()

        m = SetsNameForOneIdentifier.scrub_identifier_and_name
        assert ("A", "USE THIS NAME!") == m("A", "name a")
        assert ("B", "NAME B") == m("B", "name b")

    def test_scrub_identifier(self):
        m = Classifier.scrub_identifier
        assert None == m(None)
        assert Lowercased("Foo") == m("Foo")

    def test_scrub_name(self):
        m = Classifier.scrub_name
        assert None == m(None)
        assert Lowercased("Foo") == m("Foo")


class TestClassifierLookup:
    def test_lookup(self):
        assert DDC == Classifier.lookup(Classifier.DDC)
        assert LCC == Classifier.lookup(Classifier.LCC)
        assert LCSH == Classifier.lookup(Classifier.LCSH)
        assert FAST == Classifier.lookup(Classifier.FAST)
        assert GradeLevelClassifier == Classifier.lookup(Classifier.GRADE_LEVEL)
        assert AgeClassifier == Classifier.lookup(Classifier.AGE_RANGE)
        assert InterestLevelClassifier == Classifier.lookup(Classifier.INTEREST_LEVEL)
        assert None == Classifier.lookup("no-such-key")


class TestNestedSubgenres:
    def test_parents(self):
        assert [classifier.Romance] == list(classifier.Romantic_Suspense.parents)

        # eq_([classifier.Crime_Thrillers_Mystery, classifier.Mystery],
        #    list(classifier.Police_Procedurals.parents))

    def test_self_and_subgenres(self):
        # Fantasy
        #  - Epic Fantasy
        #  - Historical Fantasy
        #  - Urban Fantasy
        assert {
            classifier.Fantasy,
            classifier.Epic_Fantasy,
            classifier.Historical_Fantasy,
            classifier.Urban_Fantasy,
        } == set(list(classifier.Fantasy.self_and_subgenres))


class TestConsolidateWeights:
    def test_consolidate(self):
        # Asian History is a subcategory of the top-level category History.
        weights = dict()
        weights[classifier.History] = 10
        weights[classifier.Asian_History] = 4
        weights[classifier.Middle_East_History] = 1
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        assert 14 == w2[classifier.Asian_History]
        assert 1 == w2[classifier.Middle_East_History]
        assert classifier.History not in w2

        # Paranormal Romance is a subcategory of Romance, which is itself
        # a subcategory.
        weights = dict()
        weights[classifier.Romance] = 100
        weights[classifier.Paranormal_Romance] = 4
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        assert 104 == w2[classifier.Paranormal_Romance]
        assert classifier.Romance not in w2

    def test_consolidate_through_multiple_levels(self):
        # Romance is the parent of the parent of Paranormal
        # Romance, but its weight successfully flows down into
        # Paranormal Romance.
        weights = dict()
        weights[classifier.Romance] = 100
        weights[classifier.Paranormal_Romance] = 4
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        assert 104 == w2[classifier.Paranormal_Romance]
        assert classifier.Romance not in w2

    def test_consolidate_consolidates_multiple_subgenres(self):
        # This work is classified under two different sets of
        # genres/subgenres. Both of the genre/subgenre pairs get
        # rolled up properly, not just the heavier one.
        weights = dict()
        weights[classifier.Women_Detectives] = 150
        weights[classifier.Mystery] = 10
        weights[classifier.Historical_Romance] = 200
        weights[classifier.Romance] = 10
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        assert 210 == w2[classifier.Historical_Romance]
        assert 160 == w2[classifier.Women_Detectives]

    def test_consolidate_through_multiple_levels_from_multiple_sources(self):
        # This test can't work anymore because we no longer have a
        # triply-nested category like Romance/Erotica -> Romance ->
        # Paranormal Romance.
        #
        # weights = dict()
        # weights[classifier.Romance_Erotica] = 50
        # weights[classifier.Romance] = 50
        # weights[classifier.Paranormal_Romance] = 4
        # w2 = WorkClassifier.consolidate_genre_weights(weights)
        # eq_(104, w2[classifier.Paranormal_Romance])
        # assert classifier.Romance not in w2
        pass

    def test_consolidate_fails_when_threshold_not_met(self):
        weights = dict()
        weights[classifier.History] = 100
        weights[classifier.Middle_East_History] = 1
        w2 = WorkClassifier.consolidate_genre_weights(weights)
        assert 100 == w2[classifier.History]
        assert 1 == w2[classifier.Middle_East_History]


class TestFreeformAudienceClassifier:
    def test_audience(self):
        def audience(aud):
            # The second param, `name`, is not used in the audience method
            return FreeformAudienceClassifier.audience(aud, None)

        for val in ["children", "pre-adolescent", "beginning reader"]:
            assert Classifier.AUDIENCE_CHILDREN == audience(val)

        for val in [
            "young adult",
            "ya",
            "teenagers",
            "adolescent",
            "early adolescents",
        ]:
            assert Classifier.AUDIENCE_YOUNG_ADULT == audience(val)

        assert audience("adult") == Classifier.AUDIENCE_ADULT
        assert audience("adults only") == Classifier.AUDIENCE_ADULTS_ONLY
        assert audience("all ages") == Classifier.AUDIENCE_ALL_AGES
        assert audience("research") == Classifier.AUDIENCE_RESEARCH

        assert audience("books for all ages") == None

    def test_target_age(self):
        def target_age(age):
            return FreeformAudienceClassifier.target_age(age, None)

        assert target_age("beginning reader") == (5, 8)
        assert target_age("pre-adolescent") == (9, 12)
        assert target_age("all ages") == (Classifier.ALL_AGES_AGE_CUTOFF, None)

        assert target_age("babies") == (None, None)


class TestWorkClassifierFixture:
    work: Work
    identifier: Identifier
    classifier: WorkClassifier
    transaction: DatabaseTransactionFixture


@pytest.fixture()
def work_classifier_fixture(
    db,
) -> TestWorkClassifierFixture:
    fix = TestWorkClassifierFixture()
    fix.transaction = db
    fix.work = db.work(with_license_pool=True)
    fix.identifier = fix.work.presentation_edition.primary_identifier
    fix.classifier = WorkClassifier(fix.work, test_session=db.session)
    return fix


class TestWorkClassifier:
    def _genre(self, db, genre_data):
        expected_genre, ignore = Genre.lookup(db, genre_data.name)
        return expected_genre

    def test_no_assumptions(self, work_classifier_fixture: TestWorkClassifierFixture):
        """If we have no data whatsoever, we make no assumptions
        about a work's classification.
        """
        data = work_classifier_fixture
        data.classifier.weigh_metadata()
        assert None == data.classifier.fiction()
        assert None == data.classifier.audience()
        assert {} == data.classifier.genres(None)
        assert (None, None) == data.classifier.target_age(None)

    def test_weight_metadata_title(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session

        data.work.presentation_edition.title = "Star Trek: The Book"
        expected_genre = self._genre(session, classifier.Media_Tie_in_SF)
        data.classifier.weigh_metadata()
        assert 100 == data.classifier.genre_weights[expected_genre]

    def test_weight_metadata_publisher(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session

        # Genre publisher and imprint
        data.work.presentation_edition.publisher = "Harlequin"
        expected_genre = self._genre(session, classifier.Romance)
        data.classifier.weigh_metadata()
        assert 100 == data.classifier.genre_weights[expected_genre]

    def test_weight_metadata_imprint(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session

        # Imprint is more specific than publisher, so it takes precedence.
        data.work.presentation_edition.publisher = "Harlequin"
        data.work.presentation_edition.imprint = "Harlequin Intrigue"
        expected_genre = self._genre(session, classifier.Romantic_Suspense)
        general_romance = self._genre(session, classifier.Romance)

        data.classifier.weigh_metadata()
        assert general_romance not in data.classifier.genre_weights
        assert 100 == data.classifier.genre_weights[expected_genre]

    def test_metadata_implies_audience_and_genre(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # Genre and audience publisher
        data.work.presentation_edition.publisher = "Harlequin"
        data.work.presentation_edition.imprint = "Harlequin Teen"
        expected_genre = self._genre(session, classifier.Romance)

        data.classifier.weigh_metadata()
        assert 100 == data.classifier.genre_weights[expected_genre]
        assert 100 == data.classifier.audience_weights[Classifier.AUDIENCE_YOUNG_ADULT]

    def test_metadata_implies_fiction_status(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        data.work.presentation_edition.publisher = "Harlequin"
        data.work.presentation_edition.imprint = "Harlequin Nonfiction"
        data.classifier.weigh_metadata()

        assert 100 == data.classifier.fiction_weights[False]
        assert True not in data.classifier.fiction_weights

    def test_publisher_excludes_adult_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # We don't know if this is a children's book or a young adult
        # book, but we're confident it's not a book for adults.
        data.work.presentation_edition.publisher = "Scholastic Inc."

        data.classifier.weigh_metadata()
        assert -100 == data.classifier.audience_weights[Classifier.AUDIENCE_ADULT]
        assert -100 == data.classifier.audience_weights[Classifier.AUDIENCE_ADULTS_ONLY]

    def test_imprint_excludes_adult_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        data.work.presentation_edition.imprint = "Delacorte Books for Young Readers"

        data.classifier.weigh_metadata()
        assert -100 == data.classifier.audience_weights[Classifier.AUDIENCE_ADULT]
        assert -100 == data.classifier.audience_weights[Classifier.AUDIENCE_ADULTS_ONLY]

    def test_no_children_or_ya_signal_from_distributor_implies_book_is_for_adults(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session

        # Create some classifications that end up in
        # direct_from_license_source, but don't imply that the book is
        # from children or
        # YA. classifier.audience_weights[AUDIENCE_ADULT] will be set
        # to 500.
        i = data.identifier
        source = DataSource.lookup(session, DataSource.OVERDRIVE)
        for subject in ("Nonfiction", "Science Fiction", "History"):
            c = i.classify(source, Subject.OVERDRIVE, subject, weight=1000)
            data.classifier.add(c)

        # There's a little bit of evidence that it's a children's book,
        # but not enough to outweight the distributor's silence.
        c2 = data.identifier.classify(source, Subject.TAG, "Children's books", weight=1)
        data.classifier.add(c2)
        data.classifier.prepare_to_classify()
        # Overdrive classifications are regarded as 50 times more reliable
        # than their actual weight, as per Classification.scaled_weight
        assert 50000 == data.classifier.audience_weights[Classifier.AUDIENCE_ADULT]

    def test_adults_only_indication_from_distributor_has_no_implication_for_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # Create some classifications that end up in
        # direct_from_license_source, one of which implies the book is
        # for adults only.
        i = data.identifier
        source = DataSource.lookup(session, DataSource.OVERDRIVE)
        for subject in ("Erotic Literature", "Science Fiction", "History"):
            c = i.classify(source, Subject.OVERDRIVE, subject, weight=1)
            data.classifier.add(c)

        data.classifier.prepare_to_classify()

        # Again, Overdrive classifications are regarded as 50 times
        # more reliable than their actual weight, as per
        # Classification.scaled_weight
        assert 50 == data.classifier.audience_weights[Classifier.AUDIENCE_ADULTS_ONLY]

        # No boost was given to AUDIENCE_ADULT, because a distributor
        # classification implied AUDIENCE_ADULTS_ONLY.
        assert 0 == data.classifier.audience_weights[Classifier.AUDIENCE_ADULT]

    def test_no_signal_from_distributor_has_no_implication_for_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # This work has no classifications that end up in
        # direct_from_license_source. In the absence of any such
        # classifications we cannot determine whether the
        # distributor's silence about the audience is because it's a
        # book for adults or because there's just no data from the
        # distributor.
        assert {} == data.classifier.audience_weights

    def test_children_or_ya_signal_from_distributor_has_no_immediate_implication_for_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # This work has a classification direct from the distributor
        # that implies the book is for children, so no conclusions are
        # drawn in the prepare_to_classify() step.
        source = DataSource.lookup(session, DataSource.OVERDRIVE)
        c = data.identifier.classify(
            source, Subject.OVERDRIVE, "Picture Books", weight=1000
        )
        data.classifier.prepare_to_classify()
        assert {} == data.classifier.audience_weights

        data.classifier.add(c)
        assert 50000 == data.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN]

    def test_juvenile_classification_is_split_between_children_and_ya(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # LCC files both children's and YA works under 'PZ'.
        # Here's how we deal with that.
        #
        i = data.identifier
        source = DataSource.lookup(session, DataSource.OCLC)
        c = i.classify(source, Subject.LCC, "PZ", weight=100)
        data.classifier.add(c)

        # (This classification has no bearing on audience and its
        # weight will be ignored.)
        c2 = i.classify(source, Subject.TAG, "Pets", weight=1000)
        data.classifier.add(c2)
        data.classifier.prepare_to_classify
        genres, fiction, audience, target_age = data.classifier.classify()

        # Young Adult wins because we err on the side of showing books
        # to kids who are too old, rather than too young.
        assert Classifier.AUDIENCE_YOUNG_ADULT == audience

        # But behind the scenes, more is going on. The weight of the
        # classifier has been split 60/40 between YA and children.
        weights = data.classifier.audience_weights
        assert 60 == weights[Classifier.AUDIENCE_YOUNG_ADULT]
        assert 40 == weights[Classifier.AUDIENCE_CHILDREN]
        # If this is in fact a children's book, this will make it
        # relatively easy for data from some other source to come in
        # and tip the balance.

        # The adult audiences have been reduced, to reduce the chance
        # that splitting up the weight between YA and Children will
        # cause the work to be mistakenly classified as Adult.
        assert -50 == weights[Classifier.AUDIENCE_ADULT]
        assert -50 == weights[Classifier.AUDIENCE_ADULTS_ONLY]
        # The juvenile classification doesn't make the all ages less likely.
        assert 0 == weights[Classifier.AUDIENCE_ALL_AGES]

    def test_childrens_book_when_evidence_is_overwhelming(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # There is some evidence in the 'adult' and 'adults only'
        # bucket, but there's a lot more evidence that it's a
        # children's book, so we go with childrens or YA.

        # The evidence that this is a children's book is strong but
        # not overwhelming.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 10,
            Classifier.AUDIENCE_ADULTS_ONLY: 1,
            Classifier.AUDIENCE_CHILDREN: 22,
        }
        assert Classifier.AUDIENCE_ADULT == data.classifier.audience()

        # Now it's overwhelming. (the 'children' weight is more than twice
        # the combined 'adult' + 'adults only' weight.
        data.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN] = 23
        assert Classifier.AUDIENCE_CHILDREN == data.classifier.audience()

        # Now it's overwhelmingly likely to be a YA book.
        del data.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN]
        data.classifier.audience_weights[Classifier.AUDIENCE_YOUNG_ADULT] = 23
        assert Classifier.AUDIENCE_YOUNG_ADULT == data.classifier.audience()

    def test_ya_book_when_childrens_and_ya_combined_beat_adult(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # Individually, the 'children' and 'ya' buckets don't beat the
        # combined 'adult' + 'adults only' bucket by the appropriate
        # factor, but combined they do.  In this case
        # we should classify the book as YA. It might be inaccurate,
        # but it's more accurate than 'adult' and less likely to be
        # a costly mistake than 'children'.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 9,
            Classifier.AUDIENCE_ADULTS_ONLY: 0,
            Classifier.AUDIENCE_CHILDREN: 10,
            Classifier.AUDIENCE_YOUNG_ADULT: 9,
        }
        assert Classifier.AUDIENCE_YOUNG_ADULT == data.classifier.audience()

    def test_genre_may_restrict_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # The audience info says this is a YA book.
        data.classifier.audience_weights = {Classifier.AUDIENCE_YOUNG_ADULT: 1000}

        # Without any genre information, it's classified as YA.
        assert Classifier.AUDIENCE_YOUNG_ADULT == data.classifier.audience()

        # But if it's Erotica, it is always classified as Adults Only.
        genres = {classifier.Erotica: 50, classifier.Science_Fiction: 50}
        assert Classifier.AUDIENCE_ADULTS_ONLY == data.classifier.audience(genres)

    def test_all_ages_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # If the All Ages weight is more than the total adult weight and
        # the total juvenile weight, then assign all ages as the audience.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 50,
            Classifier.AUDIENCE_ADULTS_ONLY: 30,
            Classifier.AUDIENCE_ALL_AGES: 100,
            Classifier.AUDIENCE_CHILDREN: 30,
            Classifier.AUDIENCE_YOUNG_ADULT: 40,
        }
        assert Classifier.AUDIENCE_ALL_AGES == data.classifier.audience()

        # This works even if 'Children' looks much better than 'Adult'.
        # 'All Ages' looks even better than that, so it wins.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 1,
            Classifier.AUDIENCE_ADULTS_ONLY: 0,
            Classifier.AUDIENCE_ALL_AGES: 1000,
            Classifier.AUDIENCE_CHILDREN: 30,
            Classifier.AUDIENCE_YOUNG_ADULT: 29,
        }
        assert Classifier.AUDIENCE_ALL_AGES == data.classifier.audience()

        # If the All Ages weight is smaller than the total adult weight,
        # the audience is adults.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 70,
            Classifier.AUDIENCE_ADULTS_ONLY: 10,
            Classifier.AUDIENCE_ALL_AGES: 79,
            Classifier.AUDIENCE_CHILDREN: 30,
            Classifier.AUDIENCE_YOUNG_ADULT: 40,
        }
        assert Classifier.AUDIENCE_ADULT == data.classifier.audience()

    def test_research_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # If the research weight is larger than the total adult weight +
        # all ages weight and larger than the total juvenile weight +
        # all ages weight, then assign research as the audience
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 50,
            Classifier.AUDIENCE_ADULTS_ONLY: 30,
            Classifier.AUDIENCE_ALL_AGES: 10,
            Classifier.AUDIENCE_CHILDREN: 30,
            Classifier.AUDIENCE_YOUNG_ADULT: 150,
            Classifier.AUDIENCE_RESEARCH: 200,
        }
        assert Classifier.AUDIENCE_RESEARCH == data.classifier.audience()

        # If the research weight is not larger than either total adults weight
        # and all ages weight or total juvenile weight and all ages weight,
        # then we get those audience values instead.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 80,
            Classifier.AUDIENCE_ADULTS_ONLY: 10,
            Classifier.AUDIENCE_ALL_AGES: 20,
            Classifier.AUDIENCE_CHILDREN: 35,
            Classifier.AUDIENCE_YOUNG_ADULT: 40,
            Classifier.AUDIENCE_RESEARCH: 100,
        }
        assert Classifier.AUDIENCE_ADULT == data.classifier.audience()

    def test_format_classification_from_license_source_is_used(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # This book will be classified as a comic book, because
        # the "comic books" classification comes from its license source.
        source = data.work.license_pools[0].data_source
        data.identifier.classify(source, Subject.TAG, "Comic Books", weight=100)
        data.classifier.add(data.identifier.classifications[0])
        genres = data.classifier.genres(fiction=True)
        assert [(classifier.Comics_Graphic_Novels, 100)] == list(genres.items())

    def test_format_classification_not_from_license_source_is_ignored(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # This book will be not classified as a comic book, because
        # the "comic books" classification does not come from its
        # license source.
        source = data.work.license_pools[0].data_source
        oclc = DataSource.lookup(session, DataSource.OCLC)
        data.identifier.classify(oclc, Subject.TAG, "Comic Books", weight=100)
        data.classifier.add(data.identifier.classifications[0])
        genres = data.classifier.genres(fiction=True)
        assert [] == list(genres.items())

    def test_childrens_book_when_no_evidence_for_adult_book(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # There is no evidence in the 'adult' or 'adults only'
        # buckets, so minimal evidence in the 'children' bucket is
        # sufficient to be confident.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 0,
            Classifier.AUDIENCE_ADULTS_ONLY: 0,
            Classifier.AUDIENCE_CHILDREN: 1,
            Classifier.AUDIENCE_RESEARCH: 0,
            Classifier.AUDIENCE_ALL_AGES: 0,
        }
        assert Classifier.AUDIENCE_CHILDREN == data.classifier.audience()

    def test_adults_only_threshold(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # The 'adults only' weight here is not even close to a
        # majority, but it's high enough that we classify this work as
        # 'adults only' to be safe.
        data.classifier.audience_weights = {
            Classifier.AUDIENCE_ADULT: 4,
            Classifier.AUDIENCE_ADULTS_ONLY: 2,
            Classifier.AUDIENCE_CHILDREN: 4,
            Classifier.AUDIENCE_RESEARCH: 0,
            Classifier.AUDIENCE_ALL_AGES: 0,
        }
        assert Classifier.AUDIENCE_ADULTS_ONLY == data.classifier.audience()

    def test_target_age_is_default_for_adult_books(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # Target age data can't override an independently determined
        # audience.
        overdrive = DataSource.lookup(session, DataSource.OVERDRIVE)
        c1 = data.identifier.classify(
            overdrive, Subject.OVERDRIVE, "Picture Books", weight=10000
        )
        data.classifier.add(c1)

        target_age = data.classifier.target_age(Classifier.AUDIENCE_ADULT)
        assert (18, None) == target_age

    def test_target_age_weight_scaling(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # We have a weak but reliable signal that this is a book for
        # ages 5 to 7.
        overdrive = DataSource.lookup(session, DataSource.OVERDRIVE)
        c1 = data.identifier.classify(
            overdrive, Subject.OVERDRIVE, "Beginning Readers", weight=2
        )
        data.classifier.add(c1)

        # We have a louder but less reliable signal that this is a
        # book for eleven-year-olds.
        oclc = DataSource.lookup(session, DataSource.OCLC)
        c2 = data.identifier.classify(oclc, Subject.TAG, "Grade 6", weight=3)
        data.classifier.add(c2)

        # Both signals make it into the dataset, but they are weighted
        # differently, and the more reliable signal becomes stronger.
        lower = data.classifier.target_age_lower_weights
        upper = data.classifier.target_age_upper_weights
        assert lower[5] > lower[11]
        assert upper[8] > upper[11]
        assert lower[11] == upper[11]
        assert lower[5] == upper[8]

        # And this affects the target age we choose.
        a = data.classifier.target_age(Classifier.AUDIENCE_CHILDREN)
        assert (5, 8) == data.classifier.target_age(Classifier.AUDIENCE_CHILDREN)

    def test_target_age_errs_towards_wider_span(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        i = data.transaction.identifier()
        source = DataSource.lookup(session, DataSource.OVERDRIVE)
        c1 = i.classify(source, Subject.AGE_RANGE, "8-9", weight=1)
        c2 = i.classify(source, Subject.AGE_RANGE, "6-7", weight=1)

        overdrive_edition, lp = data.transaction.edition(
            data_source_name=source.name,
            with_license_pool=True,
            identifier_id=i.identifier,
        )
        data.classifier.work = data.transaction.work(
            presentation_edition=overdrive_edition
        )
        for classification in i.classifications:
            data.classifier.add(classification)
        genres, fiction, audience, target_age = data.classifier.classify()

        assert Classifier.AUDIENCE_CHILDREN == audience
        assert (6, 9) == target_age

    def test_fiction_status_restricts_genre(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session

        # Classify a book to imply that it's 50% science fiction and
        # 50% history. Then call .genres() twice. With fiction=True,
        # it's 100% science fiction. With fiction=False, it's 100% history.

        # This book is classified as 50% science fiction and 50% history.
        fiction_genre = self._genre(session, classifier.Science_Fiction)
        nonfiction_genre = self._genre(session, classifier.History)
        data.classifier.genre_weights[fiction_genre] = 100
        data.classifier.genre_weights[nonfiction_genre] = 100

        # But any given book is either fiction or nonfiction. If we say this
        # book is fiction, it's classified as 100% SF.
        genres = data.classifier.genres(True)
        assert [(fiction_genre.genredata, 100)] == list(genres.items())

        # If we say it's nonfiction, it ends up 100% history.
        genres = data.classifier.genres(False)
        assert [(nonfiction_genre.genredata, 100)] == list(genres.items())

    def test_genres_consolidated_before_classification(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session

        # A book with Romance=100, Historical Romance=5, Romantic
        # Suspense=4 will be classified by .genres() as 100%
        # Historical Romance.
        historical_romance = self._genre(session, classifier.Historical_Romance)
        romance = self._genre(session, classifier.Romance)
        romantic_suspense = self._genre(session, classifier.Romantic_Suspense)
        nonfiction_genre = self._genre(session, classifier.History)

        data.classifier.genre_weights[romance] = 100

        # Give Historical Romance enough weight to 'swallow' its
        # parent genre.  (5% of the weight of its parent.)
        data.classifier.genre_weights[historical_romance] = 5

        # Romantic Suspense does pretty well but it doesn't have
        # enough weight to swallow the parent genre, and it's
        # eliminated by the low-pass filter.
        data.classifier.genre_weights[romantic_suspense] = 4

        [genre] = list(data.classifier.genres(True).items())
        assert (historical_romance.genredata, 105) == genre

        # TODO: This behavior is a little random. As in, it's
        # random which genre comes out on top.
        #
        # data.classifier.genre_weights[romantic_suspense] = 5
        # [genre] = data.classifier.genres(True).items()
        # eq_((historical_romance.genredata, 105), genre)

    def test_overdrive_juvenile_implicit_target_age(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # An Overdrive book that is classified under "Juvenile" but
        # not under any more specific category is believed to have a
        # target age range of 9-12.
        i = data.identifier
        source = DataSource.lookup(session, DataSource.OVERDRIVE)
        c = i.classify(source, Subject.OVERDRIVE, "Juvenile Fiction", weight=1)
        data.classifier.add(c)
        data.classifier.prepare_to_classify()
        assert [9] == list(data.classifier.target_age_lower_weights.keys())
        assert [12] == list(data.classifier.target_age_upper_weights.keys())

    def test_overdrive_juvenile_explicit_target_age(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        # An Overdrive book that is classified under "Juvenile" and
        # also under some more specific category is believed to have
        # the target age range associated with that more specific
        # category.
        i = data.identifier
        source = DataSource.lookup(session, DataSource.OVERDRIVE)
        for subject in ("Juvenile Fiction", "Picture Books"):
            c = i.classify(source, Subject.OVERDRIVE, subject, weight=1)
        data.classifier.add(c)
        data.classifier.prepare_to_classify()
        assert [0] == list(data.classifier.target_age_lower_weights.keys())
        assert [4] == list(data.classifier.target_age_upper_weights.keys())

    def test_genre_low_pass_filter(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session

        romance = self._genre(session, classifier.Romance)
        data.classifier.genre_weights[romance] = 100

        sf = self._genre(session, classifier.Science_Fiction)
        data.classifier.genre_weights[sf] = 15

        # The default cutoff value of 0.15 requires that a genre have
        # a weight of at least the total weight * 0.15 to qualify.  In
        # this case, the total weight is 115 and the cutoff weight is
        # 17.25.
        [[genre, weight]] = list(data.classifier.genres(True).items())
        assert romance.genredata == genre

        # Increase SF's weight past the cutoff and we get both genres.
        data.classifier.genre_weights[sf] = 18

        [[g1, weight], [g2, weight]] = list(data.classifier.genres(True).items())
        assert {g1, g2} == {romance.genredata, sf.genredata}

    def test_classify_sets_minimum_age_high_if_minimum_lower_than_maximum(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        # We somehow end up in a situation where the proposed low end
        # of the target age is higher than the proposed high end.
        data.classifier.audience_weights[Classifier.AUDIENCE_CHILDREN] = 1
        data.classifier.target_age_lower_weights[10] = 1
        data.classifier.target_age_upper_weights[4] = 1

        # We set the low end equal to the high end, erring on the side
        # of making the book available to fewer people.
        genres, fiction, audience, target_age = data.classifier.classify()
        assert 10 == target_age[0]
        assert 10 == target_age[1]

    def test_classify_uses_default_fiction_status(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        genres, fiction, audience, target_age = data.classifier.classify(
            default_fiction=True
        )
        assert True == fiction
        genres, fiction, audience, target_age = data.classifier.classify(
            default_fiction=False
        )
        assert False == fiction
        genres, fiction, audience, target_age = data.classifier.classify(
            default_fiction=None
        )
        assert None == fiction

        # The default isn't used if there's any information about the fiction status.
        data.classifier.fiction_weights[False] = 1
        genres, fiction, audience, target_age = data.classifier.classify(
            default_fiction=None
        )
        assert False == fiction

    def test_classify_uses_default_audience(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        genres, fiction, audience, target_age = data.classifier.classify()
        assert None == audience
        genres, fiction, audience, target_age = data.classifier.classify(
            default_audience=Classifier.AUDIENCE_ADULT
        )
        assert Classifier.AUDIENCE_ADULT == audience
        genres, fiction, audience, target_age = data.classifier.classify(
            default_audience=Classifier.AUDIENCE_CHILDREN
        )
        assert Classifier.AUDIENCE_CHILDREN == audience

        # The default isn't used if there's any information about the audience.
        data.classifier.audience_weights[Classifier.AUDIENCE_ADULT] = 1
        genres, fiction, audience, target_age = data.classifier.classify(
            default_audience=None
        )
        assert Classifier.AUDIENCE_ADULT == audience

    def test_classify(self, work_classifier_fixture: TestWorkClassifierFixture):
        data = work_classifier_fixture
        session = data.transaction.session
        # At this point we've tested all the components of classify, so just
        # do an overall test to verify that classify() returns a 4-tuple
        # (genres, fiction, audience, target_age)

        data.work.presentation_edition.title = (
            "Science Fiction: A Comprehensive History"
        )
        i = data.identifier
        source = DataSource.lookup(session, DataSource.OVERDRIVE)
        c1 = i.classify(source, Subject.OVERDRIVE, "History", weight=10)
        c2 = i.classify(source, Subject.OVERDRIVE, "Science Fiction", weight=100)
        c3 = i.classify(source, Subject.OVERDRIVE, "Young Adult Nonfiction", weight=100)
        for classification in i.classifications:
            data.classifier.add(classification)
        data.classifier.prepare_to_classify()

        genres, fiction, audience, target_age = data.classifier.classify()

        # This work really looks like science fiction (w=100), but it
        # looks *even more* like nonfiction (w=100+10), and science
        # fiction is not a genre of nonfiction. So this book can't be
        # science fiction. It must be history.
        assert "History" == list(genres.keys())[0].name
        assert False == fiction
        assert Classifier.AUDIENCE_YOUNG_ADULT == audience
        assert (12, 17) == target_age

    def test_top_tier_values(self):
        c = Counter()
        assert set() == WorkClassifier.top_tier_values(c)

        c = Counter(["a"])
        assert {"a"} == WorkClassifier.top_tier_values(c)

        c = Counter([1, 1, 1, 2, 2, 3, 4, 4, 4])
        assert {1, 4} == WorkClassifier.top_tier_values(c)
        c = Counter([1, 1, 1, 2])
        assert {1} == WorkClassifier.top_tier_values(c)

    def test_duplicate_classification_ignored(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        """A given classification is only used once from
        a given data source.
        """
        data = work_classifier_fixture
        session = data.transaction.session
        history = self._genre(session, classifier.History)
        i = data.identifier
        source = DataSource.lookup(session, DataSource.AMAZON)
        c1 = i.classify(source, Subject.TAG, "History", weight=1)
        assert [] == data.classifier.classifications

        data.classifier.add(c1)
        old_weight = data.classifier.genre_weights[history]

        c2 = i.classify(source, Subject.TAG, "History", weight=100)
        data.classifier.add(c2)
        # No effect -- the weights are the same as before.
        assert old_weight == data.classifier.genre_weights[history]

        # The same classification can come in from another data source and
        # it will be taken into consideration.
        source2 = DataSource.lookup(session, DataSource.OCLC_LINKED_DATA)
        c3 = i.classify(source2, Subject.TAG, "History", weight=1)
        data.classifier.add(c3)
        assert data.classifier.genre_weights[history] > old_weight

    def test_staff_genre_overrides_others(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        genre1, is_new = Genre.lookup(session, "Psychology")
        genre2, is_new = Genre.lookup(session, "Cooking")
        subject1 = data.transaction.subject(type="type1", identifier="subject1")
        subject1.genre = genre1
        subject2 = data.transaction.subject(type="type2", identifier="subject2")
        subject2.genre = genre2
        source = DataSource.lookup(session, DataSource.AXIS_360)
        staff_source = DataSource.lookup(session, DataSource.LIBRARY_STAFF)
        classification1 = data.transaction.classification(
            identifier=data.identifier, subject=subject1, data_source=source, weight=10
        )
        classification2 = data.transaction.classification(
            identifier=data.identifier,
            subject=subject2,
            data_source=staff_source,
            weight=1,
        )
        data.classifier.add(classification1)
        data.classifier.add(classification2)
        (genre_weights, fiction, audience, target_age) = data.classifier.classify()
        assert [genre2.name] == [genre.name for genre in list(genre_weights.keys())]

    def test_staff_none_genre_overrides_others(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        source = DataSource.lookup(session, DataSource.AXIS_360)
        staff_source = DataSource.lookup(session, DataSource.LIBRARY_STAFF)
        genre1, is_new = Genre.lookup(session, "Poetry")
        subject1 = data.transaction.subject(type="type1", identifier="subject1")
        subject1.genre = genre1
        subject2 = data.transaction.subject(
            type=Subject.SIMPLIFIED_GENRE, identifier=SimplifiedGenreClassifier.NONE
        )
        classification1 = data.transaction.classification(
            identifier=data.identifier, subject=subject1, data_source=source, weight=10
        )
        classification2 = data.transaction.classification(
            identifier=data.identifier,
            subject=subject2,
            data_source=staff_source,
            weight=1,
        )
        data.classifier.add(classification1)
        data.classifier.add(classification2)
        (genre_weights, fiction, audience, target_age) = data.classifier.classify()
        assert 0 == len(list(genre_weights.keys()))

    def test_staff_fiction_overrides_others(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        source = DataSource.lookup(session, DataSource.AXIS_360)
        staff_source = DataSource.lookup(session, DataSource.LIBRARY_STAFF)
        subject1 = data.transaction.subject(type="type1", identifier="Cooking")
        subject1.fiction = False
        subject2 = data.transaction.subject(type="type2", identifier="Psychology")
        subject2.fiction = False
        subject3 = data.transaction.subject(
            type=Subject.SIMPLIFIED_FICTION_STATUS, identifier="Fiction"
        )
        classification1 = data.transaction.classification(
            identifier=data.identifier, subject=subject1, data_source=source, weight=10
        )
        classification2 = data.transaction.classification(
            identifier=data.identifier, subject=subject2, data_source=source, weight=10
        )
        classification3 = data.transaction.classification(
            identifier=data.identifier,
            subject=subject3,
            data_source=staff_source,
            weight=1,
        )
        data.classifier.add(classification1)
        data.classifier.add(classification2)
        data.classifier.add(classification3)
        (genre_weights, fiction, audience, target_age) = data.classifier.classify()
        assert True == fiction

    def test_staff_audience_overrides_others(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        pool = data.transaction.licensepool(None, data_source_name=DataSource.AXIS_360)
        license_source = pool.data_source
        staff_source = DataSource.lookup(session, DataSource.LIBRARY_STAFF)
        subject1 = data.transaction.subject(type="type1", identifier="subject1")
        subject1.audience = "Adult"
        subject2 = data.transaction.subject(type="type2", identifier="subject2")
        subject2.audience = "Adult"
        subject3 = data.transaction.subject(
            type=Subject.FREEFORM_AUDIENCE, identifier="Children"
        )
        classification1 = data.transaction.classification(
            identifier=pool.identifier,
            subject=subject1,
            data_source=license_source,
            weight=10,
        )
        classification2 = data.transaction.classification(
            identifier=pool.identifier,
            subject=subject2,
            data_source=license_source,
            weight=10,
        )
        classification3 = data.transaction.classification(
            identifier=pool.identifier,
            subject=subject3,
            data_source=staff_source,
            weight=1,
        )
        data.classifier.add(classification1)
        data.classifier.add(classification2)
        data.classifier.add(classification3)
        (genre_weights, fiction, audience, target_age) = data.classifier.classify()
        assert "Children" == audience

    def test_staff_target_age_overrides_others(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        source = DataSource.lookup(session, DataSource.AXIS_360)
        staff_source = DataSource.lookup(session, DataSource.LIBRARY_STAFF)
        subject1 = data.transaction.subject(type="type1", identifier="subject1")
        subject1.target_age = NumericRange(6, 8, "[)")
        subject1.weight_as_indicator_of_target_age = 1
        subject2 = data.transaction.subject(type="type2", identifier="subject2")
        subject2.target_age = NumericRange(6, 8, "[)")
        subject2.weight_as_indicator_of_target_age = 1
        subject3 = data.transaction.subject(type=Subject.AGE_RANGE, identifier="10-13")
        classification1 = data.transaction.classification(
            identifier=data.identifier, subject=subject1, data_source=source, weight=10
        )
        classification2 = data.transaction.classification(
            identifier=data.identifier, subject=subject2, data_source=source, weight=10
        )
        classification3 = data.transaction.classification(
            identifier=data.identifier,
            subject=subject3,
            data_source=staff_source,
            weight=1,
        )
        data.classifier.add(classification1)
        data.classifier.add(classification2)
        data.classifier.add(classification3)
        (genre_weights, fiction, audience, target_age) = data.classifier.classify()
        assert (10, 13) == target_age

    def test_not_inclusive_target_age(
        self, work_classifier_fixture: TestWorkClassifierFixture
    ):
        data = work_classifier_fixture
        session = data.transaction.session
        staff_source = DataSource.lookup(session, DataSource.LIBRARY_STAFF)
        subject = data.transaction.subject(type=Subject.AGE_RANGE, identifier="10-12")
        subject.target_age = NumericRange(9, 13, "()")
        classification = data.transaction.classification(
            identifier=data.identifier,
            subject=subject,
            data_source=staff_source,
            weight=1,
        )
        data.classifier.add(classification)
        (genre_weights, fiction, audience, target_age) = data.classifier.classify()
        assert (10, 12) == target_age
