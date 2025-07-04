# If the genre classification does not match the fiction classification, throw
# away the genre classifications.
#
# E.g. "Investigations -- nonfiction" maps to Mystery, but Mystery
# conflicts with Nonfiction.

# SQL to find commonly used DDC classifications
# select count(editions.id) as c, subjects.identifier from editions join identifiers on workrecords.primary_identifier_id=workidentifiers.id join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.type = 'DDC' and not subjects.identifier like '8%' group by subjects.identifier order by c desc;

# SQL to find commonly used classifications not assigned to a genre
# select count(identifiers.id) as c, subjects.type, substr(subjects.identifier, 0, 20) as i, substr(subjects.name, 0, 20) as n from workidentifiers join classifications on workidentifiers.id=classifications.work_identifier_id join subjects on classifications.subject_id=subjects.id where subjects.genre_id is null and subjects.fiction is null group by subjects.type, i, n order by c desc;
from __future__ import annotations

from frozendict import frozendict

from palace.manager.util.resources import resources_dir


def classifier_resources_dir():
    return resources_dir("classifier")


NO_VALUE = "NONE"
NO_NUMBER = -1


_classifiers: frozendict[str, type[Classifier]] | None = None


def lookup_classifier(scheme: str) -> type[Classifier] | None:
    """
    Look up a classifier for the given classification scheme.
    """

    global _classifiers
    if _classifiers is None:
        # Make sure that all the classifiers are imported and set up
        # the lookup dictionary.
        from palace.manager.core.classifier.age import (
            AgeClassifier,
            AgeOrGradeClassifier,
            FreeformAudienceClassifier,
            GradeLevelClassifier,
            InterestLevelClassifier,
        )
        from palace.manager.core.classifier.bic import BICClassifier
        from palace.manager.core.classifier.bisac import BISACClassifier
        from palace.manager.core.classifier.ddc import DeweyDecimalClassifier
        from palace.manager.core.classifier.gutenberg import (
            GutenbergBookshelfClassifier,
        )
        from palace.manager.core.classifier.keyword import (
            FASTClassifier,
            LCSHClassifier,
            TAGClassifier,
        )
        from palace.manager.core.classifier.lcc import LCCClassifier
        from palace.manager.core.classifier.overdrive import OverdriveClassifier
        from palace.manager.core.classifier.simplified import (
            SimplifiedFictionClassifier,
            SimplifiedGenreClassifier,
        )

        _classifiers = frozendict(
            {
                Classifier.AGE_RANGE: AgeClassifier,
                Classifier.AXIS_360_AUDIENCE: AgeOrGradeClassifier,
                Classifier.BIC: BICClassifier,
                Classifier.BISAC: BISACClassifier,
                Classifier.DDC: DeweyDecimalClassifier,
                Classifier.FAST: FASTClassifier,
                Classifier.FREEFORM_AUDIENCE: FreeformAudienceClassifier,
                Classifier.GRADE_LEVEL: GradeLevelClassifier,
                Classifier.GUTENBERG_BOOKSHELF: GutenbergBookshelfClassifier,
                Classifier.INTEREST_LEVEL: InterestLevelClassifier,
                Classifier.LCC: LCCClassifier,
                Classifier.LCSH: LCSHClassifier,
                Classifier.OVERDRIVE: OverdriveClassifier,
                Classifier.SIMPLIFIED_FICTION_STATUS: SimplifiedFictionClassifier,
                Classifier.SIMPLIFIED_GENRE: SimplifiedGenreClassifier,
                Classifier.TAG: TAGClassifier,
            }
        )

    return _classifiers.get(scheme, None)


class ClassifierConstants:
    DDC = "DDC"
    LCC = "LCC"
    LCSH = "LCSH"
    FAST = "FAST"
    OVERDRIVE = "Overdrive"
    BISAC = "BISAC"
    BIC = "BIC"
    TAG = "tag"  # Folksonomic tags.

    # Appeal controlled vocabulary developed by NYPL
    NYPL_APPEAL = "NYPL Appeal"

    GRADE_LEVEL = "Grade level"  # "1-2", "Grade 4", "Kindergarten", etc.
    AGE_RANGE = "schema:typicalAgeRange"  # "0-2", etc.
    AXIS_360_AUDIENCE = "Axis 360 Audience"

    # We know this says something about the audience but we're not sure what.
    # Could be any of the values from GRADE_LEVEL or AGE_RANGE, plus
    # "YA", "Adult", etc.
    FREEFORM_AUDIENCE = "schema:audience"

    GUTENBERG_BOOKSHELF = "gutenberg:bookshelf"
    TOPIC = "schema:Topic"
    PLACE = "schema:Place"
    PERSON = "schema:Person"
    ORGANIZATION = "schema:Organization"
    LEXILE_SCORE = "Lexile"
    ATOS_SCORE = "ATOS"
    INTEREST_LEVEL = "Interest Level"

    AUDIENCE_ADULT = "Adult"
    AUDIENCE_ADULTS_ONLY = "Adults Only"
    AUDIENCE_YOUNG_ADULT = "Young Adult"
    AUDIENCE_CHILDREN = "Children"
    AUDIENCE_ALL_AGES = "All Ages"
    AUDIENCE_RESEARCH = "Research"

    # A book for a child younger than 14 is a children's book.
    # A book for a child 14 or older is a young adult book.
    YOUNG_ADULT_AGE_CUTOFF = 14

    ADULT_AGE_CUTOFF = 18

    # "All ages" actually means "all ages with reading fluency".
    ALL_AGES_AGE_CUTOFF = 8

    AUDIENCES_YOUNG_CHILDREN = [AUDIENCE_CHILDREN, AUDIENCE_ALL_AGES]
    AUDIENCES_JUVENILE = AUDIENCES_YOUNG_CHILDREN + [AUDIENCE_YOUNG_ADULT]
    AUDIENCES_ADULT = [AUDIENCE_ADULT, AUDIENCE_ADULTS_ONLY, AUDIENCE_ALL_AGES]
    AUDIENCES = {
        AUDIENCE_ADULT,
        AUDIENCE_ADULTS_ONLY,
        AUDIENCE_YOUNG_ADULT,
        AUDIENCE_CHILDREN,
        AUDIENCE_ALL_AGES,
        AUDIENCE_RESEARCH,
    }

    SIMPLIFIED_GENRE = "http://librarysimplified.org/terms/genres/Simplified/"
    SIMPLIFIED_FICTION_STATUS = "http://librarysimplified.org/terms/fiction/"


class Classifier(ClassifierConstants):
    """Turn an external classification into an internal genre, an
    audience, an age level, and a fiction status.
    """

    AUDIENCES_NO_RESEARCH = [
        x
        for x in ClassifierConstants.AUDIENCES
        if x != ClassifierConstants.AUDIENCE_RESEARCH
    ]

    classifiers: dict[str, type[Classifier]] = {}

    @classmethod
    def range_tuple(cls, lower, upper):
        """Turn a pair of ages into a tuple that represents an age range.
        This may be turned into an inclusive postgres NumericRange later,
        but this code should not depend on postgres.
        """
        # Just in case the upper and lower ranges are mixed up,
        # and no prior code caught this, un-mix them.
        if lower and upper and lower > upper:
            lower, upper = upper, lower
        return (lower, upper)

    @classmethod
    def name_for(cls, identifier):
        """Look up a human-readable name for the given identifier."""
        return None

    @classmethod
    def classify(cls, subject):
        """Try to determine genre, audience, target age, and fiction status
        for the given Subject.
        """
        identifier, name = cls.scrub_identifier_and_name(
            subject.identifier, subject.name
        )
        fiction = cls.is_fiction(identifier, name)
        audience = cls.audience(identifier, name)

        target_age = cls.target_age(identifier, name)
        if target_age == cls.range_tuple(None, None):
            target_age = cls.default_target_age_for_audience(audience)

        return (
            cls.genre(identifier, name, fiction, audience),
            audience,
            target_age,
            fiction,
        )

    @classmethod
    def scrub_identifier_and_name(cls, identifier, name):
        """Prepare identifier and name from within a call to classify()."""
        identifier = cls.scrub_identifier(identifier)
        if isinstance(identifier, tuple):
            # scrub_identifier returned a canonical value for name as
            # well. Use it in preference to any name associated with
            # the subject.
            identifier, name = identifier
        elif not name:
            name = identifier
        name = cls.scrub_name(name)
        return identifier, name

    @classmethod
    def scrub_identifier(cls, identifier):
        """Prepare an identifier from within a call to classify().

        This may involve data normalization, conversion to lowercase,
        etc.
        """
        if identifier is None:
            return None
        return Lowercased(identifier)

    @classmethod
    def scrub_name(cls, name):
        """Prepare a name from within a call to classify()."""
        if name is None:
            return None
        return Lowercased(name)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        """Is this identifier associated with a particular Genre?"""
        return None

    @classmethod
    def genre_match(cls, query):
        """Does this query string match a particular Genre, and which part
        of the query matches?"""
        return None, None

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is this identifier+name particularly indicative of fiction?
        How about nonfiction?
        """
        if "nonfiction" in name:
            return False
        if "fiction" in name:
            return True
        return None

    @classmethod
    def audience(cls, identifier, name):
        """What does this identifier+name say about the audience for
        this book?
        """
        if "juvenile" in name:
            return cls.AUDIENCE_CHILDREN
        elif "young adult" in name or "YA" in name.original:
            return cls.AUDIENCE_YOUNG_ADULT
        return None

    @classmethod
    def audience_match(cls, query):
        """Does this query string match a particular Audience, and which
        part of the query matches?"""
        return (None, None)

    @classmethod
    def target_age(cls, identifier, name):
        """For children's books, what does this identifier+name say
        about the target age for this book?
        """
        return cls.range_tuple(None, None)

    @classmethod
    def default_target_age_for_audience(cls, audience):
        """The default target age for a given audience.

        We don't know what age range a children's book is appropriate
        for, but we can make a decent guess for a YA book, for an
        'Adult' book it's pretty clear, and for an 'Adults Only' book
        it's very clear.
        """
        if audience == Classifier.AUDIENCE_YOUNG_ADULT:
            return cls.range_tuple(14, 17)
        elif audience in (Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_ADULTS_ONLY):
            return cls.range_tuple(18, None)
        return cls.range_tuple(None, None)

    @classmethod
    def default_audience_for_target_age(cls, range):
        if range is None:
            return None
        lower = range[0]
        upper = range[1]
        if not lower and not upper:
            # You could interpret this as 'all ages' but it's more
            # likely the data is simply missing.
            return None
        if not lower:
            if upper >= cls.ADULT_AGE_CUTOFF:
                # e.g. "up to 20 years", though this doesn't
                # really make sense.
                #
                # The 'all ages' interpretation is more plausible here
                # but it's still more likely that this is simply a
                # book for grown-ups and no lower bound was provided.
                return cls.AUDIENCE_ADULT
            elif upper > cls.YOUNG_ADULT_AGE_CUTOFF:
                # e.g. "up to 15 years"
                return cls.AUDIENCE_YOUNG_ADULT
            else:
                # e.g. "up to 14 years"
                return cls.AUDIENCE_CHILDREN

        # At this point we can assume that lower is not None.
        if lower >= 18:
            return cls.AUDIENCE_ADULT
        elif lower >= cls.YOUNG_ADULT_AGE_CUTOFF:
            return cls.AUDIENCE_YOUNG_ADULT
        elif lower <= cls.ALL_AGES_AGE_CUTOFF and (
            upper is not None and upper >= cls.ADULT_AGE_CUTOFF
        ):
            # e.g. "for children ages 7-77". The 'all ages' reading
            # is here the most plausible.
            return cls.AUDIENCE_ALL_AGES
        elif lower >= 12 and (not upper or upper >= cls.YOUNG_ADULT_AGE_CUTOFF):
            # Although we treat "Young Adult" as starting at 14, many
            # outside sources treat it as starting at 12. As such we
            # treat "12 and up" or "12-14" as an indicator of a Young
            # Adult audience, with a target age that overlaps what we
            # consider a Children audience.
            return cls.AUDIENCE_YOUNG_ADULT
        else:
            return cls.AUDIENCE_CHILDREN

    @classmethod
    def and_up(cls, young, keyword):
        """Encapsulates the logic of what "[x] and up" actually means.

        Given the lower end of an age range, tries to determine the
        upper end of the range.
        """
        if young is None:
            return None
        if not any([keyword.endswith(x) for x in ("and up", "and up.", "+", "+.")]):
            return None

        if young >= 18:
            old = young
        elif young >= 12:
            # "12 and up", "14 and up", etc.  are
            # generally intended to cover the entire
            # YA span.
            old = 17
        elif young >= 8:
            # "8 and up" means something like "8-12"
            old = young + 4
        else:
            # Whereas "3 and up" really means more
            # like "3 to 5".
            old = young + 2
        return old


# This is the large-scale structure of our classification system.
#
# If the name of a genre is a string, it's the name of the genre
# and there are no subgenres.
#
# If the name of a genre is a dictionary, the 'name' argument is the
# name of the genre, and the 'subgenres' argument is the list of the
# subgenres.

COMICS_AND_GRAPHIC_NOVELS = "Comics & Graphic Novels"

fiction_genres = [
    "Adventure",
    "Classics",
    COMICS_AND_GRAPHIC_NOVELS,
    "Drama",
    dict(name="Erotica", audiences=Classifier.AUDIENCE_ADULTS_ONLY),
    dict(
        name="Fantasy",
        subgenres=[
            "Epic Fantasy",
            "Historical Fantasy",
            "Urban Fantasy",
        ],
    ),
    "Folklore",
    "Historical Fiction",
    dict(
        name="Horror",
        subgenres=[
            "Gothic Horror",
            "Ghost Stories",
            "Vampires",
            "Werewolves",
            "Occult Horror",
        ],
    ),
    "Humorous Fiction",
    "Literary Fiction",
    "LGBTQ Fiction",
    dict(
        name="Mystery",
        subgenres=[
            "Crime & Detective Stories",
            "Hard-Boiled Mystery",
            "Police Procedural",
            "Cozy Mystery",
            "Historical Mystery",
            "Paranormal Mystery",
            "Women Detectives",
        ],
    ),
    "Poetry",
    "Religious Fiction",
    dict(
        name="Romance",
        subgenres=[
            "Contemporary Romance",
            "Gothic Romance",
            "Historical Romance",
            "Paranormal Romance",
            "Western Romance",
            "Romantic Suspense",
        ],
    ),
    dict(
        name="Science Fiction",
        subgenres=[
            "Dystopian SF",
            "Space Opera",
            "Cyberpunk",
            "Military SF",
            "Alternative History",
            "Steampunk",
            "Romantic SF",
            "Media Tie-in SF",
        ],
    ),
    "Short Stories",
    dict(
        name="Suspense/Thriller",
        subgenres=[
            "Historical Thriller",
            "Espionage",
            "Supernatural Thriller",
            "Medical Thriller",
            "Political Thriller",
            "Psychological Thriller",
            "Technothriller",
            "Legal Thriller",
            "Military Thriller",
        ],
    ),
    "Urban Fiction",
    "Westerns",
    "Women's Fiction",
]

nonfiction_genres = [
    dict(
        name="Art & Design",
        subgenres=[
            "Architecture",
            "Art",
            "Art Criticism & Theory",
            "Art History",
            "Design",
            "Fashion",
            "Photography",
        ],
    ),
    "Biography & Memoir",
    "Education",
    dict(
        name="Personal Finance & Business",
        subgenres=[
            "Business",
            "Economics",
            "Management & Leadership",
            "Personal Finance & Investing",
            "Real Estate",
        ],
    ),
    dict(
        name="Parenting & Family",
        subgenres=[
            "Family & Relationships",
            "Parenting",
        ],
    ),
    dict(
        name="Food & Health",
        subgenres=[
            "Bartending & Cocktails",
            "Cooking",
            "Health & Diet",
            "Vegetarian & Vegan",
        ],
    ),
    dict(
        name="History",
        subgenres=[
            "African History",
            "Ancient History",
            "Asian History",
            "Civil War History",
            "European History",
            "Latin American History",
            "Medieval History",
            "Middle East History",
            "Military History",
            "Modern History",
            "Renaissance & Early Modern History",
            "United States History",
            "World History",
        ],
    ),
    dict(
        name="Hobbies & Home",
        subgenres=[
            "Antiques & Collectibles",
            "Crafts & Hobbies",
            "Gardening",
            "Games",
            "House & Home",
            "Pets",
        ],
    ),
    "Humorous Nonfiction",
    dict(
        name="Entertainment",
        subgenres=[
            "Film & TV",
            "Music",
            "Performing Arts",
        ],
    ),
    "Life Strategies",
    "Literary Criticism",
    "Periodicals",
    "Philosophy",
    "Political Science",
    dict(
        name="Reference & Study Aids",
        subgenres=[
            "Dictionaries",
            "Foreign Language Study",
            "Law",
            "Study Aids",
        ],
    ),
    dict(
        name="Religion & Spirituality",
        subgenres=[
            "Body, Mind & Spirit",
            "Buddhism",
            "Christianity",
            "Hinduism",
            "Islam",
            "Judaism",
        ],
    ),
    dict(
        name="Science & Technology",
        subgenres=[
            "Computers",
            "Mathematics",
            "Medical",
            "Nature",
            "Psychology",
            "Science",
            "Social Sciences",
            "Technology",
        ],
    ),
    "Self-Help",
    "Sports",
    "Travel",
    "True Crime",
]


class GenreData:
    def __init__(self, name, is_fiction, parent=None, audience_restriction=None):
        self.name = name
        self.parent = parent
        self.is_fiction = is_fiction
        self.subgenres = []
        if isinstance(audience_restriction, str):
            audience_restriction = [audience_restriction]
        self.audience_restriction = audience_restriction

    def __repr__(self):
        return "<GenreData: %s>" % self.name

    @property
    def self_and_subgenres(self):
        yield self
        yield from self.all_subgenres

    @property
    def all_subgenres(self):
        for child in self.subgenres:
            yield from child.self_and_subgenres

    @property
    def parents(self):
        parents = []
        p = self.parent
        while p:
            parents.append(p)
            p = p.parent
        return reversed(parents)

    def has_subgenre(self, subgenre):
        for s in self.subgenres:
            if s == subgenre or s.has_subgenre(subgenre):
                return True
        return False

    @property
    def variable_name(self):
        return (
            self.name.replace("-", "_")
            .replace(", & ", "_")
            .replace(", ", "_")
            .replace(" & ", "_")
            .replace(" ", "_")
            .replace("/", "_")
            .replace("'", "")
        )

    @classmethod
    def populate(cls, namespace, genres, fiction_source, nonfiction_source):
        """Create a GenreData object for every genre and subgenre in the given
        list of fiction and nonfiction genres.
        """
        for source, default_fiction in (
            (fiction_source, True),
            (nonfiction_source, False),
        ):
            for item in source:
                subgenres = []
                audience_restriction = None
                name = item
                fiction = default_fiction
                if isinstance(item, dict):
                    name = item["name"]
                    subgenres = item.get("subgenres", [])
                    audience_restriction = item.get("audience_restriction")
                    fiction = item.get("fiction", default_fiction)

                cls.add_genre(
                    namespace,
                    genres,
                    name,
                    subgenres,
                    fiction,
                    None,
                    audience_restriction,
                )

    @classmethod
    def add_genre(
        cls, namespace, genres, name, subgenres, fiction, parent, audience_restriction
    ):
        """Create a GenreData object. Add it to a dictionary and a namespace."""
        if isinstance(name, tuple):
            name, default_fiction = name
        default_fiction = None
        default_audience = None
        if parent:
            default_fiction = parent.is_fiction
            default_audience = parent.audience_restriction
        if isinstance(name, dict):
            data = name
            subgenres = data.get("subgenres", [])
            name = data["name"]
            fiction = data.get("fiction", default_fiction)
            audience_restriction = data.get("audience", default_audience)
        if name in genres:
            raise ValueError("Duplicate genre name! %s" % name)

        # Create the GenreData object.
        genre_data = GenreData(name, fiction, parent, audience_restriction)
        if parent:
            parent.subgenres.append(genre_data)

        # Add the genre to the given dictionary, keyed on name.
        genres[genre_data.name] = genre_data

        # Convert the name to a Python-safe variable name,
        # and add it to the given namespace.
        namespace[genre_data.variable_name] = genre_data

        # Do the same for subgenres.
        for sub in subgenres:
            cls.add_genre(
                namespace, genres, sub, [], fiction, genre_data, audience_restriction
            )


genres: dict[str, GenreData] = dict()
genres_by_variable_name: dict[str, GenreData] = dict()
GenreData.populate(genres_by_variable_name, genres, fiction_genres, nonfiction_genres)


# The structure of this module is to make all the GenreData objects available,
# for import anywhere in the codebase, as attributes of this module. This used
# to be done by adding them all to globals(), but this made type checking with
# mypy difficult. So instead we add them to a dictionary and then use __getattr__
# to make them available as attributes of the module. This lets mypy know that
# unknown attributes are actually GenreData objects.
#
# See this stackoverflow answer for more details:
# https://stackoverflow.com/questions/60739889/bypass-mypys-module-has-no-attribute-on-dynamic-attribute-setting
#
# TODO: Eventually I'd like to refactor this, so we don't have to use __getattr__
#   here, and can just import the GenreData objects directly.
def __getattr__(name: str) -> GenreData:
    return genres_by_variable_name[name]


class Lowercased(str):
    """A lowercased string that remembers its original value."""

    def __new__(cls, value):
        if isinstance(value, Lowercased):
            # Nothing to do.
            return value
        if not isinstance(value, str):
            value = str(value)
        new_value = value.lower()
        if new_value.endswith("."):
            new_value = new_value[:-1]
        o = super().__new__(cls, new_value)
        o.original = value
        return o

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier
