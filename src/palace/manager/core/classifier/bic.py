from palace.manager.core import classifier
from palace.manager.core.classifier import Classifier


class BICClassifier(Classifier):
    # These prefixes came from from http://editeur.dyndns.org/bic_categories

    LEVEL_1_PREFIXES = {
        classifier.Art_Design: "A",
        classifier.Biography_Memoir: "B",
        classifier.Foreign_Language_Study: "C",
        classifier.Literary_Criticism: "D",
        classifier.Reference_Study_Aids: "G",
        classifier.Social_Sciences: "J",
        classifier.Personal_Finance_Business: "K",
        classifier.Law: "L",
        classifier.Medical: "M",
        classifier.Science_Technology: "P",
        classifier.Technology: "T",
        classifier.Computers: "U",
    }

    LEVEL_2_PREFIXES = {
        classifier.Art_History: "AC",
        classifier.Photography: "AJ",
        classifier.Design: "AK",
        classifier.Architecture: "AM",
        classifier.Film_TV: "AP",
        classifier.Performing_Arts: "AS",
        classifier.Music: "AV",
        classifier.Poetry: "DC",
        classifier.Drama: "DD",
        classifier.Classics: "FC",
        classifier.Mystery: "FF",
        classifier.Suspense_Thriller: "FH",
        classifier.Adventure: "FJ",
        classifier.Horror: "FK",
        classifier.Science_Fiction: "FL",
        classifier.Fantasy: "FM",
        classifier.Erotica: "FP",
        classifier.Romance: "FR",
        classifier.Historical_Fiction: "FV",
        classifier.Religious_Fiction: "FW",
        classifier.Comics_Graphic_Novels: "FX",
        classifier.History: "HB",
        classifier.Philosophy: "HP",
        classifier.Religion_Spirituality: "HR",
        classifier.Psychology: "JM",
        classifier.Education: "JN",
        classifier.Political_Science: "JP",
        classifier.Economics: "KC",
        classifier.Business: "KJ",
        classifier.Mathematics: "PB",
        classifier.Science: "PD",
        classifier.Self_Help: "VS",
        classifier.Body_Mind_Spirit: "VX",
        classifier.Food_Health: "WB",
        classifier.Antiques_Collectibles: "WC",
        classifier.Crafts_Hobbies: "WF",
        classifier.Humorous_Nonfiction: "WH",
        classifier.House_Home: "WK",
        classifier.Gardening: "WM",
        classifier.Nature: "WN",
        classifier.Sports: "WS",
        classifier.Travel: "WT",
    }

    LEVEL_3_PREFIXES = {
        classifier.Historical_Mystery: "FFH",
        classifier.Espionage: "FHD",
        classifier.Westerns: "FJW",
        classifier.Space_Opera: "FLS",
        classifier.Historical_Romance: "FRH",
        classifier.Short_Stories: "FYB",
        classifier.World_History: "HBG",
        classifier.Military_History: "HBW",
        classifier.Christianity: "HRC",
        classifier.Buddhism: "HRE",
        classifier.Hinduism: "HRG",
        classifier.Islam: "HRH",
        classifier.Judaism: "HRJ",
        classifier.Fashion: "WJF",
        classifier.Poetry: "YDP",
        classifier.Adventure: "YFC",
        classifier.Horror: "YFD",
        classifier.Science_Fiction: "YFG",
        classifier.Fantasy: "YFH",
        classifier.Romance: "YFM",
        classifier.Humorous_Fiction: "YFQ",
        classifier.Historical_Fiction: "YFT",
        classifier.Comics_Graphic_Novels: "YFW",
        classifier.Art: "YNA",
        classifier.Music: "YNC",
        classifier.Performing_Arts: "YND",
        classifier.Film_TV: "YNF",
        classifier.History: "YNH",
        classifier.Nature: "YNN",
        classifier.Religion_Spirituality: "YNR",
        classifier.Science_Technology: "YNT",
        classifier.Humorous_Nonfiction: "YNU",
        classifier.Sports: "YNW",
    }

    LEVEL_4_PREFIXES = {
        classifier.European_History: "HBJD",
        classifier.Asian_History: "HBJF",
        classifier.African_History: "HBJH",
        classifier.Ancient_History: "HBLA",
        classifier.Modern_History: "HBLL",
        classifier.Drama: "YNDS",
        classifier.Comics_Graphic_Novels: "YNUC",
    }

    PREFIX_LISTS = [
        LEVEL_4_PREFIXES,
        LEVEL_3_PREFIXES,
        LEVEL_2_PREFIXES,
        LEVEL_1_PREFIXES,
    ]

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier.startswith("f") or identifier.startswith("yf"):
            return True
        return False

    @classmethod
    def audience(cls, identifier, name):
        # BIC doesn't distinguish children's and YA.
        # Classify it as YA to be safe.
        if identifier.startswith("y"):
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for prefixes in cls.PREFIX_LISTS:
            for l, v in list(prefixes.items()):
                if identifier.startswith(v.lower()):
                    return l
        return None


Classifier.classifiers[Classifier.BIC] = BICClassifier
