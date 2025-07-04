import json
import re

from palace.manager.core import classifier


class LCCClassifier(classifier.Classifier):
    TOP_LEVEL = re.compile("^([A-Z]{1,2})")
    FICTION = {"PN", "PQ", "PR", "PS", "PT", "PZ"}
    JUVENILE = {"PZ"}

    GENRES = {
        # Unclassified/complicated stuff.
        # "America": E11-E143
        # Ancient_History: D51-D90
        # Angling: SH401-SH691
        # Civil_War_History: E456-E655
        # Geography: leftovers of G
        # Islam: BP1-BP253
        # Latin_American_History: F1201-F3799
        # Medieval History: D111-D203
        # Military_History: D25-D27
        # Modern_History: ???
        # Renaissance_History: D219-D234 (1435-1648, so roughly)
        # Sports: GV557-1198.995
        # TODO: E and F are actually "the Americas".
        # United_States_History is E151-E909, F1-F975 but not E456-E655
        classifier.African_History: ["DT"],
        classifier.Ancient_History: ["DE"],
        classifier.Architecture: ["NA"],
        classifier.Art_Criticism_Theory: ["BH"],
        classifier.Asian_History: ["DS", "DU"],
        classifier.Biography_Memoir: ["CT"],
        classifier.Business: ["HC", "HF", "HJ"],
        classifier.Christianity: ["BR", "BS", "BT", "BV", "BX"],
        classifier.Cooking: ["TX"],
        classifier.Crafts_Hobbies: ["TT"],
        classifier.Economics: ["HB"],
        classifier.Education: ["L"],
        classifier.European_History: [
            "DA",
            "DAW",
            "DB",
            "DD",
            "DF",
            "DG",
            "DH",
            "DJ",
            "DK",
            "DL",
            "DP",
            "DQ",
            "DR",
        ],
        classifier.Folklore: ["GR"],
        classifier.Games: ["GV"],
        classifier.Islam: ["BP"],
        classifier.Judaism: ["BM"],
        classifier.Literary_Criticism: ["Z"],
        classifier.Mathematics: ["QA", "HA", "GA"],
        classifier.Medical: ["QM", "R"],
        classifier.Military_History: ["U", "V"],
        classifier.Music: ["M"],
        classifier.Parenting_Family: ["HQ"],
        classifier.Periodicals: ["AP", "AN"],
        classifier.Philosophy: ["BC", "BD", "BJ"],
        classifier.Photography: ["TR"],
        classifier.Political_Science: ["J", "HX"],
        classifier.Psychology: ["BF"],
        classifier.Reference_Study_Aids: ["AE", "AG", "AI"],
        classifier.Religion_Spirituality: ["BL", "BQ"],
        classifier.Science: [
            "QB",
            "QC",
            "QD",
            "QE",
            "QH",
            "QK",
            "QL",
            "QR",
            "CC",
            "GB",
            "GC",
            "QP",
        ],
        classifier.Social_Sciences: [
            "HD",
            "HE",
            "HF",
            "HM",
            "HN",
            "HS",
            "HT",
            "HV",
            "GN",
            "GF",
            "GT",
        ],
        classifier.Sports: ["SK"],
        classifier.World_History: ["CB"],
    }

    LEFTOVERS = dict(
        B=classifier.Philosophy,
        T=classifier.Technology,
        Q=classifier.Science,
        S=classifier.Science,
        H=classifier.Social_Sciences,
        D=classifier.History,
        N=classifier.Art,
        L=classifier.Education,
        E=classifier.United_States_History,
        F=classifier.United_States_History,
        BP=classifier.Religion_Spirituality,
    )

    NAMES = json.loads(
        classifier.classifier_resources_dir().joinpath("lcc_one_level.json").read_text()
    )

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier
        return identifier.upper()

    @classmethod
    def name_for(cls, identifier):
        return cls.NAMES.get(identifier, None)

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier == "P":
            return True
        if not identifier.startswith("P"):
            return False
        for i in cls.FICTION:
            if identifier.startswith(i):
                return True
        return False

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for genre, strings in list(cls.GENRES.items()):
            for s in strings:
                if identifier.startswith(s):
                    return genre
        for prefix, genre in list(cls.LEFTOVERS.items()):
            if identifier.startswith(prefix):
                return genre
        return None

    @classmethod
    def audience(cls, identifier, name):
        if identifier.startswith("PZ"):
            return cls.AUDIENCE_CHILDREN

        # Everything else is _supposedly_ for adults, but we don't
        # trust that assumption.
        return None
