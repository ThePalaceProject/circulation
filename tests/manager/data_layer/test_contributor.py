from functools import partial

from palace.manager.data_layer.contributor import ContributorData
from palace.manager.sqlalchemy.model.contributor import Contributor
from tests.fixtures.database import DatabaseTransactionFixture


class TestContributorData:
    def test__init__(self):
        # Roles defaults to AUTHOR
        assert ContributorData().roles == (Contributor.Role.AUTHOR,)

        # If sort_name is given then it is used as the cached value
        assert ContributorData(sort_name="foo")._cached_sort_name == "foo"

        # Test that ContributorData is hashable
        hash(ContributorData())

    def test_from_contribution(self, db: DatabaseTransactionFixture):
        # Makes sure ContributorData.from_contribution copies all the fields over.

        # make author with that name, add author to list and pass to edition
        contributors = ["PrimaryAuthor"]
        edition, pool = db.edition(with_license_pool=True, authors=contributors)

        contribution = edition.contributions[0]
        contributor = contribution.contributor
        contributor.lc = "1234567"
        contributor.viaf = "ABC123"
        contributor.aliases = ["Primo"]
        contributor.display_name = "Test Author For The Win"
        contributor.family_name = "TestAuttie"
        contributor.wikipedia_name = "TestWikiAuth"
        contributor.biography = "He was born on Main Street."
        contributor.extra[Contributor.BIRTH_DATE] = "2001-01-01"

        contributor_data = ContributorData.from_contribution(contribution)

        # make sure contributor fields are still what I expect
        assert contributor_data.lc == contributor.lc
        assert contributor_data.viaf == contributor.viaf
        assert contributor_data.aliases == tuple(contributor.aliases)
        assert contributor_data.display_name == contributor.display_name
        assert contributor_data.family_name == contributor.family_name
        assert contributor_data.wikipedia_name == contributor.wikipedia_name
        assert contributor_data.biography == contributor.biography
        assert contributor_data.extra == contributor.extra

    def test_lookup(self, db: DatabaseTransactionFixture):
        # Test the method that uses the database to gather as much
        # self-consistent information as possible about a person.
        lookup = partial(ContributorData.lookup, db.session)

        # We know very little about this person.
        db_al, _ = db.contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
        )
        al = ContributorData.from_contributor(db_al)

        # We know a lot about this person.
        db_pkd, _ = db.contributor(
            sort_name="Dick, Phillip K.",
            display_name="Phillip K. Dick",
            viaf="27063583",
            lc="n79018147",
        )
        pkd = ContributorData.from_contributor(db_pkd)

        # If there's no Contributor that matches the request, the method
        # returns None.
        assert lookup(sort_name="Marenghi, Garth") is None

        # If one and only one Contributor matches the request, the method
        # returns a ContributorData with all necessary information.
        assert lookup(display_name="Phillip K. Dick") == pkd
        assert lookup(sort_name="Dick, Phillip K.") == pkd
        assert lookup(viaf="27063583") == pkd
        assert lookup(lc="n79018147") == pkd

        # If we're able to identify a Contributor from part of the
        # input, then any contradictory input is ignored in favor of
        # what we know from the database.
        assert (
            lookup(
                display_name="Phillip K. Dick",
                sort_name="Marenghi, Garth",
                viaf="1234",
                lc="abcd",
            )
            == pkd
        )

        # If we're able to identify a Contributor, but we don't know some
        # of the information, those fields are left blank.
        assert lookup(display_name="Ann Leckie") == al

        # Now let's test cases where the database lookup finds
        # multiple Contributors.

        # An exact duplicate of an existing Contributor changes
        # nothing.
        db.contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
        )
        assert lookup(display_name="Ann Leckie") == al

        # If there's a duplicate that adds more information, multiple
        # records are consolidated, creating a synthetic
        # ContributorData that doesn't correspond to any one
        # Contributor.
        db.contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
            viaf="73520345",
        )

        expect = ContributorData(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
            viaf="73520345",
            roles=[],
        )
        assert lookup(display_name="Ann Leckie") == expect

        # Again, this works even if some of the incoming arguments
        # turn out not to be supported by the database db.
        assert (
            lookup(display_name="Ann Leckie", sort_name="Ann Leckie", viaf="abcd")
            == expect
        )

        # If there's a duplicate that provides conflicting information,
        # the corresponding field is left blank -- we don't know which
        # value is correct.
        db.contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
            viaf="abcd",
        )
        assert lookup(display_name="Ann Leckie") == al

        # If there's conflicting information in the database for a
        # field, but the input included a value for that field, then
        # the input value is used.
        assert lookup(display_name="Ann Leckie", viaf="73520345") == expect

    def test_apply(self, db: DatabaseTransactionFixture):
        # Makes sure ContributorData.apply copies all the fields over when there's changes to be made.

        contributor_old, made_new = db.contributor(
            sort_name="Doe, John", viaf="viaf12345"
        )

        kwargs = dict()
        kwargs[Contributor.BIRTH_DATE] = "2001-01-01"

        contributor_data = ContributorData(
            sort_name="Doerr, John",
            lc="1234567",
            viaf="ABC123",
            aliases=["Primo"],
            display_name="Test Author For The Win",
            family_name="TestAuttie",
            wikipedia_name="TestWikiAuth",
            biography="He was born on Main Street.",
            extra=kwargs,
        )

        contributor_new, changed = contributor_data.apply(contributor_old)

        assert changed == True
        assert contributor_new.sort_name == "Doerr, John"
        assert contributor_new.lc == "1234567"
        assert contributor_new.viaf == "ABC123"
        assert contributor_new.aliases == ["Primo"]
        assert contributor_new.display_name == "Test Author For The Win"
        assert contributor_new.family_name == "TestAuttie"
        assert contributor_new.wikipedia_name == "TestWikiAuth"
        assert contributor_new.biography == "He was born on Main Street."

        assert contributor_new.extra[Contributor.BIRTH_DATE] == "2001-01-01"
        # assert_equal(contributor_new.contributions, "Audio")

        contributor_new, changed = contributor_data.apply(contributor_new)
        assert changed == False

    def test_display_name_to_sort_name_from_existing_contributor(
        self, db: DatabaseTransactionFixture
    ):
        # If there's an existing contributor with a matching display name,
        # we'll use their sort name.
        existing_contributor, ignore = db.contributor(
            sort_name="Sort, Name", display_name="John Doe"
        )
        assert (
            "Sort, Name"
            == ContributorData.display_name_to_sort_name_from_existing_contributor(
                db.session, "John Doe"
            )
        )

        # Otherwise, we don't know.
        assert (
            None
            == ContributorData.display_name_to_sort_name_from_existing_contributor(
                db.session, "Jane Doe"
            )
        )

    def test_find_sort_name(self, db: DatabaseTransactionFixture):
        db.contributor(sort_name="Author, E.", display_name="Existing Author")

        # If there's already a sort name, keep it.
        contributor_data = ContributorData(sort_name="Sort Name")
        assert contributor_data.find_sort_name(db.session) == "Sort Name"

        contributor_data = ContributorData(
            sort_name="Sort Name", display_name="Existing Author"
        )
        assert contributor_data.find_sort_name(db.session) == "Sort Name"

        contributor_data = ContributorData(
            sort_name="Sort Name", display_name="Metadata Client Author"
        )
        assert contributor_data.find_sort_name(db.session) == "Sort Name"

        # If there's no sort name but there's already an author with the same display name,
        # use that author's sort name.
        contributor_data = ContributorData(display_name="Existing Author")
        assert contributor_data.find_sort_name(db.session) == "Author, E."

        # If there's no sort name, no existing author, and nothing from the metadata
        # wrangler, guess the sort name based on the display name.
        contributor_data = ContributorData(display_name="New Author")
        assert contributor_data.find_sort_name(db.session) == "Author, New"
