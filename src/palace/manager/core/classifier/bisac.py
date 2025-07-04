import csv
import re

from palace.manager.core import classifier
from palace.manager.core.classifier import (
    Classifier,
    Lowercased,
    classifier_resources_dir,
)
from palace.manager.core.classifier.keyword import KeywordBasedClassifier


class CustomMatchToken:
    """A custom token used in matching rules."""

    def matches(self, subject_token):
        """Does the given token match this one?"""
        raise NotImplementedError()


class Something(CustomMatchToken):
    """A CustomMatchToken that will match any single token."""

    def matches(self, subject_token):
        return True


class RE(CustomMatchToken):
    """A CustomMatchToken that performs a regular expression search."""

    def __init__(self, pattern):
        self.re = re.compile(pattern, re.I)

    def matches(self, subject_token):
        return self.re.search(subject_token)


class Interchangeable(CustomMatchToken):
    """A token that matches a list of strings."""

    def __init__(self, *choices):
        """All of these strings are interchangeable for matching purposes."""
        self.choices = {Lowercased(x) for x in choices}

    def matches(self, subject_token):
        return Lowercased(subject_token) in self.choices


# Special tokens for use in matching rules.
something = Something()
fiction = Interchangeable("Juvenile Fiction", "Young Adult Fiction", "Fiction")
juvenile = Interchangeable("Juvenile Fiction", "Juvenile Nonfiction")
ya = Interchangeable("Young Adult Fiction", "Young Adult Nonfiction")

# These need special code because they can modify the token stack.
anything = object()
nonfiction = object()

# These are BISAC categories that changed their names. We want to treat both
# names as equivalent. In most cases, the name change is cosmetic.
body_mind_spirit = Interchangeable("Body, Mind & Spirit", "Mind & Spirit")
psychology = Interchangeable("Psychology", "Psychology & Psychiatry")
technology = Interchangeable("Technology & Engineering", "Technology")
social_topics = Interchangeable("Social Situations", "Social Topics")

# This name change is _not_ cosmetic. The category was split into
# two, and we're putting everything that was in the old category into
# one of the two.
literary_criticism = Interchangeable(
    "Literary Criticism", "Literary Criticism & Collections"
)

# If these variables are used in a rule, they must be the first token in
# that rule.
special_variables = {
    nonfiction: "nonfiction",
    fiction: "fiction",
    juvenile: "juvenile",
    ya: "ya",
}


class MatchingRule:
    """A rule that takes a list of subject parts and returns
    an appropriate classification.
    """

    def __init__(self, result, *ruleset):
        if result is None:
            raise ValueError(
                "MatchingRule returns None on a non-match, it can't also return None on a match."
            )

        self.result = result
        self.ruleset = []

        # Track the subjects that were 'caught' by this rule,
        # for debugging purposes.
        self.caught = []

        for i, rule in enumerate(ruleset):
            if i > 0 and rule in special_variables:
                raise ValueError(
                    "Special token '%s' must be the first in a ruleset."
                    % special_variables[rule]
                )

            if isinstance(rule, (bytes, str)):
                # It's a string. We do case-insensitive comparisons,
                # so lowercase it.
                self.ruleset.append(Lowercased(rule))
            else:
                # It's a special object. Add it to the ruleset as-is.
                self.ruleset.append(rule)

    def match(self, *subject):
        """If `subject` matches this ruleset, return the appropriate
        result. Otherwise, return None.
        """
        # Create parallel lists of the subject and the things it has to
        # match.
        must_match = list(self.ruleset)
        remaining_subject = list(subject)

        # Consume tokens from both lists until we've confirmed no
        # match or there is nothing left to match.
        match_so_far = True
        while match_so_far and must_match:
            match_so_far, must_match, remaining_subject = self._consume(
                must_match, remaining_subject
            )

        if match_so_far:
            # Everything that had to match, did.
            self.caught.append(subject)
            return self.result

        # Something that had to match, didn't.
        return None

    def _consume(self, rules, subject):
        """The first token (and possibly more) of the rules must match the
        first token (and possibly more) of the subject.

        All matched rule and subject tokens are consumed.

        :return: A 3-tuple (could_match, new_rules, new_subject)

        could_match is a boolean that is False if we now know that the
        subject does not match the rule, and True if it might still
        match the rule.

        new_rules contains the tokens in the ruleset that have yet to
        be activated.

        new_subject contains the tokens in the subject that have yet
        to be checked.
        """
        if not rules:
            # An empty ruleset matches everything.
            return True, rules, subject

        if not subject and rules != [anything]:
            # Apart from [anything], no non-empty ruleset matches an
            # empty subject.
            return False, rules, subject

        # Figure out which rule we'll be applying. We won't need it
        # again, so we can remove it from the ruleset.
        rule_token = rules.pop(0)
        if rule_token == anything:
            # This is the complicated one.

            if not rules:
                # If the final rule is 'anything', then that's redundant,
                # but we can declare success and stop.
                return True, rules, subject

            # At this point we know that 'anything' is followed by some
            # other rule token.
            next_rule = rules.pop(0)

            # We can consume as many subject tokens as necessary, but
            # eventually a subject token must match this subsequent
            # rule token.
            while subject:
                subject_token = subject.pop(0)
                submatch, ignore1, ignore2 = self._consume([next_rule], [subject_token])
                if submatch:
                    # We had to remove some number of subject tokens,
                    # but we found one that matches the next rule.
                    return True, rules, subject
                else:
                    # That token didn't match, but maybe the next one will.
                    pass

            # We went through the entire remaining subject and didn't
            # find a match for the rule token that follows 'anything'.
            return False, rules, subject

        # We're comparing two individual tokens.
        subject_token = subject.pop(0)
        if isinstance(rule_token, CustomMatchToken):
            match = rule_token.matches(subject_token)
        elif rule_token == nonfiction:
            # This is too complex to be a CustomMatchToken because
            # we may be modifying the subject token list.
            match = subject_token not in (
                "juvenile fiction",
                "young adult fiction",
                "fiction",
            )
            if match and subject_token not in (
                "juvenile nonfiction",
                "young adult nonfiction",
            ):
                # The implicit top-level lane is 'nonfiction',
                # which means we popped a token like 'History' that
                # needs to go back on the stack.
                subject.insert(0, subject_token)
        else:
            # The strings must match exactly.
            match = rule_token == subject_token
        return match, rules, subject


def m(result, *ruleset):
    """Alias for the MatchingRule constructor with a short name."""
    return MatchingRule(result, *ruleset)


class BISACClassifier(Classifier):
    """Handle real, genuine, according-to-Hoyle BISAC classifications.

    Subclasses of this method can use the same basic classification logic
    to classify classifications that are based on BISAC but have cosmetic
    differences.

    First, a BISAC code is mapped to its human-readable name.

    Second, the name is split into parts (e.g. ["Fiction", "War &
    Military"]).

    To determine fiction status, audience, target age, or genre, the
    list of name parts is compared against each of a list of matching
    rules.
    """

    # Map identifiers to human-readable names.
    with classifier_resources_dir().joinpath("bisac.csv").open() as f:
        NAMES = dict([i.strip() for i in l] for l in csv.reader(f))

    # Indicates that even though this rule doesn't match a subject, no
    # further rules in the same category should be run on it, because they
    # will lead to inaccurate information.
    stop = object()

    # If none of these rules match, a lane's fiction status depends on the
    # genre assigned to it.
    FICTION = [
        m(True, "Fiction"),
        m(True, "Juvenile Fiction"),
        m(False, "Juvenile Nonfiction"),
        m(True, "Young Adult Fiction"),
        m(False, "Young Adult Nonfiction"),
        m(False, anything, "Essays"),
        m(False, anything, "Letters"),
        m(True, "Literary Collections"),
        m(stop, "Humor"),
        m(stop, "Drama"),
        m(stop, "Poetry"),
        m(False, anything),
    ]

    # In BISAC, juvenile fiction and YA fiction are kept in separate
    # spaces. Nearly everything outside that space can be presumed to
    # have AUDIENCE_ADULT.
    AUDIENCE = [
        m(Classifier.AUDIENCE_CHILDREN, "Bibles", anything, "Children"),
        m(Classifier.AUDIENCE_CHILDREN, juvenile, anything),
        m(Classifier.AUDIENCE_YOUNG_ADULT, ya, anything),
        m(Classifier.AUDIENCE_YOUNG_ADULT, "Bibles", anything, "Youth & Teen"),
        m(Classifier.AUDIENCE_ADULTS_ONLY, anything, "Erotica"),
        m(Classifier.AUDIENCE_ADULTS_ONLY, "Humor", "Topic", "Adult"),
        m(Classifier.AUDIENCE_ADULT, anything),
    ]

    TARGET_AGE = [
        m((0, 4), juvenile, anything, "Readers", "Beginner"),
        m((5, 7), juvenile, anything, "Readers", "Intermediate"),
        m((5, 7), juvenile, anything, "Early Readers"),
        m((8, 13), juvenile, anything, "Chapter Books"),
    ]

    GENRE = [
        # Put all erotica in Erotica, to keep the other lanes at
        # "Adult" level or lower.
        m(classifier.Erotica, anything, "Erotica"),
        # Put all non-erotica comics into the same bucket, regardless
        # of their content.
        m(classifier.Comics_Graphic_Novels, "Comics & Graphic Novels"),
        m(classifier.Comics_Graphic_Novels, nonfiction, "Comics & Graphic Novels"),
        m(classifier.Comics_Graphic_Novels, fiction, "Comics & Graphic Novels"),
        # "Literary Criticism / Foo" implies Literary Criticism, not Foo.
        m(classifier.Literary_Criticism, anything, literary_criticism),
        # "Fiction / Christian / Foo" implies Religious Fiction
        # more strongly than it implies Foo.
        m(classifier.Religious_Fiction, fiction, anything, "Christian"),
        # "Fiction / Foo / Short Stories" implies Short Stories more
        # strongly than it implies Foo. This assumes that a short
        # story collection within a genre will also be classified
        # separately under that genre. This could definitely be
        # improved but would require a Subject to map to multiple
        # Genres.
        m(classifier.Short_Stories, fiction, anything, RE("^Anthologies")),
        m(classifier.Short_Stories, fiction, anything, RE("^Short Stories")),
        m(classifier.Short_Stories, "Literary Collections"),
        m(classifier.Short_Stories, fiction, anything, "Collections & Anthologies"),
        # Classify top-level fiction categories into fiction genres.
        #
        # First, handle large overarching genres that have subgenres
        # and adjacent genres.
        #
        # Fantasy
        m(classifier.Epic_Fantasy, fiction, "Fantasy", "Epic"),
        m(classifier.Historical_Fantasy, fiction, "Fantasy", "Historical"),
        m(classifier.Urban_Fantasy, fiction, "Fantasy", "Urban"),
        m(classifier.Fantasy, fiction, "Fantasy"),
        m(classifier.Fantasy, fiction, "Romance", "Fantasy"),
        m(classifier.Fantasy, fiction, "Sagas"),
        # Mystery
        # n.b. no BISAC for Paranormal_Mystery
        m(
            classifier.Crime_Detective_Stories,
            fiction,
            "Mystery & Detective",
            "Private Investigators",
        ),
        m(classifier.Crime_Detective_Stories, fiction, "Crime"),
        m(classifier.Crime_Detective_Stories, fiction, "Thrillers", "Crime"),
        m(
            classifier.Hard_Boiled_Mystery,
            fiction,
            "Mystery & Detective",
            "Hard-Boiled",
        ),
        m(
            classifier.Police_Procedural,
            fiction,
            "Mystery & Detective",
            "Police Procedural",
        ),
        m(classifier.Cozy_Mystery, fiction, "Mystery & Detective", "Cozy"),
        m(classifier.Historical_Mystery, fiction, "Mystery & Detective", "Historical"),
        m(classifier.Women_Detectives, fiction, "Mystery & Detective", "Women Sleuths"),
        m(classifier.Mystery, fiction, anything, "Mystery & Detective"),
        # Horror
        m(classifier.Ghost_Stories, fiction, "Ghost"),
        m(classifier.Occult_Horror, fiction, "Occult & Supernatural"),
        m(classifier.Gothic_Horror, fiction, "Gothic"),
        m(classifier.Horror, fiction, "Horror"),
        # Romance
        # n.b. no BISAC for Gothic Romance
        m(classifier.Contemporary_Romance, fiction, "Romance", "Contemporary"),
        m(classifier.Historical_Romance, fiction, "Romance", "Historical"),
        m(classifier.Paranormal_Romance, fiction, "Romance", "Paranormal"),
        m(classifier.Western_Romance, fiction, "Romance", "Western"),
        m(classifier.Romantic_Suspense, fiction, "Romance", "Suspense"),
        m(classifier.Romantic_SF, fiction, "Romance", "Time Travel"),
        m(classifier.Romantic_SF, fiction, "Romance", "Science Fiction"),
        m(classifier.Romance, fiction, "Romance"),
        # Science fiction
        # n.b. no BISAC for Cyberpunk
        m(classifier.Dystopian_SF, fiction, "Dystopian"),
        m(classifier.Space_Opera, fiction, "Science Fiction", "Space Opera"),
        m(classifier.Military_SF, fiction, "Science Fiction", "Military"),
        m(classifier.Alternative_History, fiction, "Alternative History"),
        # Juvenile steampunk is classified directly beneath 'fiction'.
        m(classifier.Steampunk, fiction, anything, "Steampunk"),
        m(classifier.Science_Fiction, fiction, "Science Fiction"),
        # Thrillers
        # n.b. no BISAC for Supernatural_Thriller
        m(classifier.Historical_Thriller, fiction, "Thrillers", "Historical"),
        m(classifier.Espionage, fiction, "Thrillers", "Espionage"),
        m(classifier.Medical_Thriller, fiction, "Thrillers", "Medical"),
        m(classifier.Political_Thriller, fiction, "Thrillers", "Political"),
        m(classifier.Legal_Thriller, fiction, "Thrillers", "Legal"),
        m(classifier.Technothriller, fiction, "Thrillers", "Technological"),
        m(classifier.Military_Thriller, fiction, "Thrillers", "Military"),
        m(classifier.Suspense_Thriller, fiction, "Thrillers"),
        # Then handle the less complicated genres of fiction.
        m(classifier.Adventure, fiction, "Action & Adventure"),
        m(classifier.Adventure, fiction, "Sea Stories"),
        m(classifier.Adventure, fiction, "War & Military"),
        m(classifier.Classics, fiction, "Classics"),
        m(classifier.Folklore, fiction, "Fairy Tales, Folk Tales, Legends & Mythology"),
        m(classifier.Historical_Fiction, anything, "Historical"),
        m(classifier.Humorous_Fiction, fiction, "Humorous"),
        m(classifier.Humorous_Fiction, fiction, "Satire"),
        m(classifier.Literary_Fiction, fiction, "Literary"),
        m(classifier.LGBTQ_Fiction, fiction, "Gay"),
        m(classifier.LGBTQ_Fiction, fiction, "Lesbian"),
        m(classifier.LGBTQ_Fiction, fiction, "Gay & Lesbian"),
        m(classifier.Religious_Fiction, fiction, "Religious"),
        m(classifier.Religious_Fiction, fiction, "Jewish"),
        m(classifier.Religious_Fiction, fiction, "Visionary & Metaphysical"),
        m(classifier.Womens_Fiction, fiction, anything, "Contemporary Women"),
        m(classifier.Westerns, fiction, "Westerns"),
        # n.b. BISAC "Fiction / Urban" is distinct from "Fiction /
        # African-American / Urban", and does not map to any of our
        # genres.
        m(classifier.Urban_Fiction, fiction, "African American", "Urban"),
        # BISAC classifies these genres at the top level, which we
        # treat as 'nonfiction', but we classify them as fiction. It
        # doesn't matter because they're neither, really.
        m(classifier.Drama, nonfiction, "Drama"),
        m(classifier.Poetry, nonfiction, "Poetry"),
        # Now on to nonfiction.
        # Classify top-level nonfiction categories into fiction genres.
        #
        # First, handle large overarching genres that have subgenres
        # and adjacent genres.
        #
        # Art & Design
        m(classifier.Architecture, nonfiction, "Architecture"),
        m(classifier.Art_Criticism_Theory, nonfiction, "Art", "Criticism & Theory"),
        m(classifier.Art_History, nonfiction, "Art", "History"),
        m(classifier.Fashion, nonfiction, "Design", "Fashion"),
        m(classifier.Design, nonfiction, "Design"),
        m(classifier.Art_Design, nonfiction, "Art"),
        m(classifier.Photography, nonfiction, "Photography"),
        # Personal Finance & Business
        m(classifier.Business, nonfiction, "Business & Economics", RE("^Business.*")),
        m(classifier.Business, nonfiction, "Business & Economics", "Accounting"),
        m(classifier.Economics, nonfiction, "Business & Economics", "Economics"),
        m(
            classifier.Economics,
            nonfiction,
            "Business & Economics",
            "Environmental Economics",
        ),
        m(classifier.Economics, nonfiction, "Business & Economics", RE("^Econo.*")),
        m(
            classifier.Management_Leadership,
            nonfiction,
            "Business & Economics",
            "Management",
        ),
        m(
            classifier.Management_Leadership,
            nonfiction,
            "Business & Economics",
            "Management Science",
        ),
        m(
            classifier.Management_Leadership,
            nonfiction,
            "Business & Economics",
            "Leadership",
        ),
        m(
            classifier.Personal_Finance_Investing,
            nonfiction,
            "Business & Economics",
            "Personal Finance",
        ),
        m(
            classifier.Personal_Finance_Investing,
            nonfiction,
            "Business & Economics",
            "Personal Success",
        ),
        m(
            classifier.Personal_Finance_Investing,
            nonfiction,
            "Business & Economics",
            "Investments & Securities",
        ),
        m(classifier.Real_Estate, nonfiction, "Business & Economics", "Real Estate"),
        m(classifier.Personal_Finance_Business, nonfiction, "Business & Economics"),
        # Parenting & Family
        m(classifier.Parenting, nonfiction, "Family & Relationships", "Parenting"),
        m(classifier.Family_Relationships, nonfiction, "Family & Relationships"),
        # Food & Health
        m(classifier.Bartending_Cocktails, nonfiction, "Cooking", "Beverages"),
        m(classifier.Health_Diet, nonfiction, "Cooking", "Health & Healing"),
        m(classifier.Health_Diet, nonfiction, "Health & Fitness"),
        m(classifier.Vegetarian_Vegan, nonfiction, "Cooking", "Vegetarian & Vegan"),
        m(classifier.Cooking, nonfiction, "Cooking"),
        # History
        m(classifier.African_History, nonfiction, "History", "Africa"),
        m(classifier.Ancient_History, nonfiction, "History", "Ancient"),
        m(classifier.Asian_History, nonfiction, "History", "Asia"),
        m(
            classifier.Civil_War_History,
            nonfiction,
            "History",
            "United States",
            RE("^Civil War"),
        ),
        m(classifier.European_History, nonfiction, "History", "Europe"),
        m(classifier.Latin_American_History, nonfiction, "History", "Latin America"),
        m(classifier.Medieval_History, nonfiction, "History", "Medieval"),
        m(classifier.Military_History, nonfiction, "History", "Military"),
        m(
            classifier.Renaissance_Early_Modern_History,
            nonfiction,
            "History",
            "Renaissance",
        ),
        m(
            classifier.Renaissance_Early_Modern_History,
            nonfiction,
            "History",
            "Modern",
            RE("^1[678]th Century"),
        ),
        m(classifier.Modern_History, nonfiction, "History", "Modern"),
        m(classifier.United_States_History, nonfiction, "History", "Native American"),
        m(classifier.United_States_History, nonfiction, "History", "United States"),
        m(classifier.World_History, nonfiction, "History", "World"),
        m(classifier.World_History, nonfiction, "History", "Civilization"),
        m(classifier.History, nonfiction, "History"),
        # Hobbies & Home
        m(classifier.Antiques_Collectibles, nonfiction, "Antiques & Collectibles"),
        m(classifier.Crafts_Hobbies, nonfiction, "Crafts & Hobbies"),
        m(classifier.Gardening, nonfiction, "Gardening"),
        m(classifier.Games, nonfiction, "Games"),
        m(classifier.House_Home, nonfiction, "House & Home"),
        m(classifier.Pets, nonfiction, "Pets"),
        # Entertainment
        m(classifier.Film_TV, nonfiction, "Performing Arts", "Film & Video"),
        m(classifier.Film_TV, nonfiction, "Performing Arts", "Television"),
        m(classifier.Music, nonfiction, "Music"),
        m(classifier.Performing_Arts, nonfiction, "Performing Arts"),
        # Reference & Study Aids
        m(classifier.Dictionaries, nonfiction, "Reference", "Dictionaries"),
        m(classifier.Foreign_Language_Study, nonfiction, "Foreign Language Study"),
        m(classifier.Law, nonfiction, "Law"),
        m(classifier.Study_Aids, nonfiction, "Study Aids"),
        m(classifier.Reference_Study_Aids, nonfiction, "Reference"),
        m(classifier.Reference_Study_Aids, nonfiction, "Language Arts & Disciplines"),
        # Religion & Spirituality
        m(classifier.Body_Mind_Spirit, nonfiction, body_mind_spirit),
        m(classifier.Buddhism, nonfiction, "Religion", "Buddhism"),
        m(classifier.Christianity, nonfiction, "Religion", RE("^Biblical")),
        m(classifier.Christianity, nonfiction, "Religion", RE("^Christian")),
        m(classifier.Christianity, nonfiction, "Bibles"),
        m(classifier.Hinduism, nonfiction, "Religion", "Hinduism"),
        m(classifier.Islam, nonfiction, "Religion", "Islam"),
        m(classifier.Judaism, nonfiction, "Religion", "Judaism"),
        m(classifier.Religion_Spirituality, nonfiction, "Religion"),
        # Science & Technology
        m(classifier.Computers, nonfiction, "Computers"),
        m(classifier.Mathematics, nonfiction, "Mathematics"),
        m(classifier.Medical, nonfiction, "Medical"),
        m(classifier.Nature, nonfiction, "Nature"),
        m(classifier.Psychology, nonfiction, psychology),
        m(
            classifier.Political_Science,
            nonfiction,
            "Social Science",
            "Politics & Government",
        ),
        m(classifier.Social_Sciences, nonfiction, "Social Science"),
        m(classifier.Technology, nonfiction, technology),
        m(classifier.Technology, nonfiction, "Transportation"),
        m(classifier.Science, nonfiction, "Science"),
        # Then handle the less complicated genres of nonfiction.
        # n.b. no BISAC for Periodicals.
        # n.b. no BISAC for Humorous Nonfiction per se.
        m(
            classifier.Music,
            nonfiction,
            "Biography & Autobiography",
            "Composers & Musicians",
        ),
        m(
            classifier.Entertainment,
            nonfiction,
            "Biography & Autobiography",
            "Entertainment & Performing Arts",
        ),
        m(classifier.Biography_Memoir, nonfiction, "Biography & Autobiography"),
        m(classifier.Education, nonfiction, "Education"),
        m(classifier.Philosophy, nonfiction, "Philosophy"),
        m(classifier.Political_Science, nonfiction, "Political Science"),
        m(classifier.Self_Help, nonfiction, "Self-Help"),
        m(classifier.Sports, nonfiction, "Sports & Recreation"),
        m(classifier.Travel, nonfiction, "Travel"),
        m(classifier.True_Crime, nonfiction, "True Crime"),
        # Handle cases where Juvenile/YA uses different terms than
        # would be used for the same books for adults.
        m(classifier.Business, nonfiction, "Careers"),
        m(classifier.Christianity, nonfiction, "Religious", "Christian"),
        m(classifier.Cooking, nonfiction, "Cooking & Food"),
        m(classifier.Education, nonfiction, "School & Education"),
        m(classifier.Family_Relationships, nonfiction, "Family"),
        m(classifier.Fantasy, fiction, "Fantasy & Magic"),
        m(classifier.Ghost_Stories, fiction, "Ghost Stories"),
        m(classifier.Fantasy, fiction, "Magical Realism"),
        m(classifier.Fantasy, fiction, "Mermaids"),
        m(classifier.Fashion, nonfiction, "Fashion"),
        m(classifier.Folklore, fiction, "Fairy Tales & Folklore"),
        m(classifier.Folklore, fiction, "Legends, Myths, Fables"),
        m(classifier.Games, nonfiction, "Games & Activities"),
        m(classifier.Health_Diet, nonfiction, "Health & Daily Living"),
        m(classifier.Horror, fiction, "Horror & Ghost Stories"),
        m(classifier.Horror, fiction, "Monsters"),
        m(classifier.Horror, fiction, "Paranormal"),
        m(classifier.Horror, fiction, "Paranormal, Occult & Supernatural"),
        m(classifier.Horror, fiction, "Vampires"),
        m(classifier.Horror, fiction, "Werewolves & Shifters"),
        m(classifier.Horror, fiction, "Zombies"),
        m(classifier.Humorous_Fiction, fiction, "Humorous Stories"),
        m(classifier.Humorous_Nonfiction, "Young Adult Nonfiction", "Humor"),
        m(classifier.LGBTQ_Fiction, fiction, "LGBT"),
        m(classifier.Law, nonfiction, "Law & Crime"),
        m(classifier.Mystery, fiction, "Mysteries & Detective Stories"),
        m(classifier.Nature, nonfiction, "Animals"),
        m(classifier.Personal_Finance_Investing, nonfiction, "Personal Finance"),
        m(classifier.Poetry, fiction, "Nursery Rhymes"),
        m(classifier.Poetry, fiction, "Stories in Verse"),
        m(classifier.Poetry, fiction, "Novels in Verse"),
        m(classifier.Poetry, fiction, "Poetry"),
        m(classifier.Reference_Study_Aids, nonfiction, "Language Arts"),
        m(classifier.Romance, fiction, "Love & Romance"),
        m(classifier.Science_Fiction, fiction, "Robots"),
        m(classifier.Science_Fiction, fiction, "Time Travel"),
        m(classifier.Social_Sciences, nonfiction, "Media Studies"),
        m(classifier.Suspense_Thriller, fiction, "Superheroes"),
        m(classifier.Suspense_Thriller, fiction, "Thrillers & Suspense"),
        # Most of the subcategories of 'Science & Nature' go into Nature,
        # but these go into Science.
        m(classifier.Science, nonfiction, "Science & Nature", "Discoveries"),
        m(classifier.Science, nonfiction, "Science & Nature", "Experiments & Projects"),
        m(classifier.Science, nonfiction, "Science & Nature", "History of Science"),
        m(classifier.Science, nonfiction, "Science & Nature", "Physics"),
        m(classifier.Science, nonfiction, "Science & Nature", "Weights & Measures"),
        m(classifier.Science, nonfiction, "Science & Nature", "General"),
        # Any other subcategory of 'Science & Nature' goes under Nature
        m(classifier.Nature, nonfiction, "Science & Nature", something),
        # Life Strategies is juvenile/YA-specific, and contains both
        # fiction and nonfiction. It's called "Social Issues" for
        # juvenile fiction/nonfiction, and "Social Topics" for YA
        # nonfiction. "Social Themes" in YA fiction is _not_
        # classified as Life Strategies.
        m(classifier.Life_Strategies, fiction, "social issues"),
        m(classifier.Life_Strategies, nonfiction, "social issues"),
        m(classifier.Life_Strategies, nonfiction, social_topics),
    ]

    @classmethod
    def is_fiction(cls, identifier, name):
        for ruleset in cls.FICTION:
            fiction = ruleset.match(*name)
            if fiction is cls.stop:
                return None
            if fiction is not None:
                return fiction
        keyword = "/".join(name)
        return KeywordBasedClassifier.is_fiction(identifier, keyword)

    @classmethod
    def audience(cls, identifier, name):
        for ruleset in cls.AUDIENCE:
            audience = ruleset.match(*name)
            if audience is cls.stop:
                return None
            if audience is not None:
                return audience
        keyword = "/".join(name)
        return KeywordBasedClassifier.audience(identifier, keyword)

    @classmethod
    def target_age(cls, identifier, name):
        for ruleset in cls.TARGET_AGE:
            target_age = ruleset.match(*name)
            if target_age is cls.stop:
                return None
            if target_age is not None:
                return target_age

        # If all else fails, try the keyword-based classifier.
        keyword = "/".join(name)
        return KeywordBasedClassifier.target_age(identifier, keyword)

    @classmethod
    def genre(cls, identifier, name, fiction, audience):
        for ruleset in cls.GENRE:
            genre = ruleset.match(*name)
            if genre is cls.stop:
                return None
            if genre is not None:
                return genre

        # If all else fails, try a keyword-based classifier.
        keyword = "/".join(name)
        return KeywordBasedClassifier.genre(identifier, keyword, fiction, audience)

    # A BISAC name copied from the BISAC website may end with this
    # human-readable note, which is not part of the official name.
    see_also = re.compile(r"\(see also .*")

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier
        if identifier.startswith("FB"):
            identifier = identifier[2:]
        if identifier in cls.NAMES:
            # We know the canonical name for this BISAC identifier,
            # and we are better equipped to classify the canonical
            # names, so use the canonical name in preference to
            # whatever name the distributor provided.
            return (identifier, cls.NAMES[identifier])
        return identifier

    @classmethod
    def scrub_name(cls, name):
        """Split the name into a list of lowercase keywords."""

        # All of our comparisons are case-insensitive.
        name = Lowercased(name)

        # Take corrective action to finame a number of common problems
        # seen in the wild.
        #

        # A comma may have been replaced with a space.
        name = name.replace("  ", ", ")

        # The name may be enclosed in an extra set of quotes.
        for quote in "'\"":
            if name.startswith(quote):
                name = name[1:]
            if name.endswith(quote):
                name = name[:-1]

        # The name may end with an extraneous marker character or
        # (if it was copied from the BISAC website) an asterisk.
        for separator in "|/*":
            if name.endswith(separator):
                name = name[:-1]

        # A name copied from the BISAC website may end with a
        # human-readable cross-reference.
        name = cls.see_also.sub("", name)

        # The canonical separator character is a slash, but a pipe
        # has also been used.
        for separator in "|/":
            if separator in name:
                parts = [name.strip() for name in name.split(separator) if name.strip()]
                break
        else:
            parts = [name]
        return parts
