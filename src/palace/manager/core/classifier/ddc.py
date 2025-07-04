import json

from palace.manager.core import classifier
from palace.manager.core.classifier import Classifier


class DeweyDecimalClassifier(Classifier):
    NAMES = json.loads(
        classifier.classifier_resources_dir().joinpath("dewey_1000.json").read_text()
    )

    # Add some other values commonly found in MARC records.
    NAMES["B"] = "Biography"
    NAMES["E"] = "Juvenile Fiction"
    NAMES["F"] = "Fiction"
    NAMES["FIC"] = "Fiction"
    NAMES["J"] = "Juvenile Nonfiction"
    NAMES["Y"] = "Young Adult"

    FICTION = {813, 823, 833, 843, 853, 863, 873, 883, "FIC", "E", "F"}
    NONFICTION = {"J", "B"}

    # 791.4572 and 791.4372 is for recordings. 741.59 is for comic
    #  adaptations? This is a good sign that a identifier should
    #  not be considered, actually.
    # 428.6 - Primers, Readers, i.e. collections of stories
    # 700 - Arts - full of distinctions
    # 700.8996073 - African American arts
    # 700.9 - Art history
    # 700.71 Arts education
    # 398.7 Jokes and jests

    GENRES = {
        classifier.African_History: list(range(960, 970)),
        classifier.Architecture: list(range(710, 720)) + list(range(720, 730)),
        classifier.Art: list(range(700, 710)) + list(range(730, 770)) + [774, 776],
        classifier.Art_Criticism_Theory: [701],
        classifier.Asian_History: list(range(950, 960)) + [995, 996, 997],
        classifier.Biography_Memoir: ["B", 920],
        classifier.Economics: list(range(330, 340)),
        classifier.Christianity: [list(range(220, 230)) + list(range(230, 290))],
        classifier.Cooking: [list(range(640, 642))],
        classifier.Performing_Arts: [790, 791, 792],
        classifier.Entertainment: 790,
        classifier.Games: [793, 794, 795],
        classifier.Drama: [812, 822, 832, 842, 852, 862, 872, 882],
        classifier.Education: list(range(370, 380)) + [707],
        classifier.European_History: list(range(940, 950)),
        classifier.Folklore: [398],
        classifier.History: [900],
        classifier.Islam: [297],
        classifier.Judaism: [296],
        classifier.Latin_American_History: list(range(981, 990)),
        classifier.Law: list(range(340, 350)) + [364],
        classifier.Management_Leadership: [658],
        classifier.Mathematics: list(range(510, 520)),
        classifier.Medical: list(range(610, 620)),
        classifier.Military_History: list(range(355, 360)),
        classifier.Music: list(range(780, 789)),
        classifier.Periodicals: list(range(50, 60))
        + [105, 405, 505, 605, 705, 805, 905],
        classifier.Philosophy: list(range(160, 200)),
        classifier.Photography: [771, 772, 773, 775, 778, 779],
        classifier.Poetry: [811, 821, 831, 841, 851, 861, 871, 874, 881, 884],
        classifier.Political_Science: list(range(320, 330)) + list(range(351, 355)),
        classifier.Psychology: list(range(150, 160)),
        classifier.Foreign_Language_Study: list(range(430, 500)),
        classifier.Reference_Study_Aids: list(range(10, 20))
        + list(range(30, 40))
        + [103, 203, 303, 403, 503, 603, 703, 803, 903]
        + list(range(410, 430)),
        classifier.Religion_Spirituality: list(range(200, 220))
        + [290, 292, 293, 294, 295, 299],
        classifier.Science: (
            [500, 501, 502]
            + list(range(506, 510))
            + list(range(520, 530))
            + list(range(530, 540))
            + list(range(540, 550))
            + list(range(550, 560))
            + list(range(560, 570))
            + list(range(570, 580))
            + list(range(580, 590))
            + list(range(590, 600))
        ),
        classifier.Social_Sciences: (
            list(range(300, 310))
            + list(range(360, 364))
            + list(range(390, 397))
            + [399]
        ),
        classifier.Sports: list(range(796, 800)),
        classifier.Technology: (
            [600, 601, 602, 604]
            + list(range(606, 610))
            + list(range(610, 640))
            + list(range(660, 670))
            + list(range(670, 680))
            + list(range(681, 690))
            + list(range(690, 700))
        ),
        classifier.Travel: list(range(910, 920)),
        classifier.United_States_History: list(range(973, 980)),
        classifier.World_History: [909],
    }

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier
        if isinstance(identifier, int):
            identifier = str(identifier).zfill(3)

        identifier = identifier.upper()

        if identifier.startswith("[") and identifier.endswith("]"):
            # This is just bad data.
            identifier = identifier[1:-1]

        if identifier.startswith("C") or identifier.startswith("A"):
            # A work from our Canadian neighbors or our Australian
            # friends.
            identifier = identifier[1:]
        elif identifier.startswith("NZ"):
            # A work from the good people of New Zealand.
            identifier = identifier[2:]

        # Trim everything after the first period. We don't know how to
        # deal with it.
        if "." in identifier:
            identifier = identifier.split(".")[0]
        try:
            identifier = int(identifier)
        except ValueError:
            pass

        # For our purposes, Dewey Decimal numbers are identifiers
        # without names.
        return identifier, None

    @classmethod
    def is_fiction(cls, identifier, name):
        """Is the given DDC classification likely to contain fiction?"""
        if identifier == "Y":
            # Inconsistently used for young adult fiction and
            # young adult nonfiction.
            return None

        if isinstance(identifier, (bytes, str)) and (
            identifier.startswith("Y") or identifier.startswith("J")
        ):
            # Young adult/children's literature--not necessarily fiction
            identifier = identifier[1:]
            try:
                identifier = int(identifier)
            except ValueError:
                pass

        if identifier in cls.FICTION:
            return True
        if identifier in cls.NONFICTION:
            return False

        # TODO: Make NONFICTION more comprehensive and return None if
        # not in there, instead of always returning False. Or maybe
        # returning False is fine here, who knows.
        return False

    @classmethod
    def audience(cls, identifier, name):
        if identifier == "E":
            # Juvenile fiction
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, (bytes, str)) and identifier.startswith("J"):
            return cls.AUDIENCE_CHILDREN

        if isinstance(identifier, (bytes, str)) and identifier.startswith("Y"):
            return cls.AUDIENCE_YOUNG_ADULT

        if isinstance(identifier, (bytes, str)) and identifier == "FIC":
            # FIC is used for all types of fiction.
            return None

        # Everything else is _supposedly_ for adults, but we don't
        # trust that assumption.
        return None

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for genre, identifiers in list(cls.GENRES.items()):
            if identifier == identifiers or (
                isinstance(identifiers, list) and identifier in identifiers
            ):
                return genre
        return None
