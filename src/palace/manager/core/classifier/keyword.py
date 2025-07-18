import re
from collections import Counter

from palace.manager.core import classifier
from palace.manager.core.classifier.age import AgeOrGradeClassifier


def match_kw(*l):
    """Turn a list of strings into a function which uses a regular expression
    to match any of those strings, so long as there's a word boundary on both ends.
    The function will match all the strings by default, or can exclude the strings
    that are examples of the classification.
    """

    def match_term(term, exclude_examples=False):
        if not l:
            return None
        if exclude_examples:
            keywords = [keyword for keyword in l if not isinstance(keyword, Eg)]
        else:
            keywords = [str(keyword) for keyword in l]

        if not keywords:
            return None
        any_keyword = "|".join(keywords)
        with_boundaries = r"\b(%s)\b" % any_keyword
        return re.compile(with_boundaries, re.I).search(term)

    # This is a dictionary so it can be used as a class variable
    return {"search": match_term}


class Eg:
    """Mark this string as an example of a classification, rather than
    an exact identifier for that classification. For example, basketball
    is an an example of a sport, but athletics is an identifier for the sports
    classification.
    """

    def __init__(self, term):
        self.term = term

    def __str__(self):
        return self.term


class KeywordBasedClassifier(AgeOrGradeClassifier):
    """Classify a book based on keywords."""

    # We have to handle these first because otherwise '\bfiction\b'
    # will match it.
    LEVEL_1_NONFICTION_INDICATORS = match_kw("non-fiction", "non fiction")

    LEVEL_2_FICTION_INDICATORS = match_kw(
        "fiction",
        Eg("stories"),
        Eg("tales"),
        Eg("literature"),
        Eg("bildungsromans"),
        "fictitious",
    )
    LEVEL_2_NONFICTION_INDICATORS = match_kw(
        Eg("history"),
        Eg("biography"),
        Eg("histories"),
        Eg("biographies"),
        Eg("autobiography"),
        Eg("autobiographies"),
        "nonfiction",
        Eg("essays"),
        Eg("letters"),
        Eg("true story"),
        Eg("personal memoirs"),
    )
    JUVENILE_INDICATORS = match_kw(
        "for children", "children's", "juvenile", Eg("nursery rhymes"), Eg("9-12")
    )
    YOUNG_ADULT_INDICATORS = match_kw(
        "young adult",
        "ya",
        "12-Up",
        "teenage .*fiction",
        "teens .*fiction",
        "teen books",
        Eg("teenage romance"),
    )

    # Children's books don't generally deal with romance, so although
    # "Juvenile Fiction" generally refers to children's fiction,
    # "Juvenile Fiction / Love & Romance" is almost certainly YA.
    JUVENILE_TERMS_THAT_IMPLY_YOUNG_ADULT = {
        "love & romance",
        "romance",
        "romantic",
    }

    # These identifiers indicate that the string "children" or
    # "juvenile" in the identifier does not actually mean the work is
    # _for_ children.
    JUVENILE_BLACKLIST = {
        "military participation",
        "services",
        "children's accidents",
        "children's voices",
        "juvenile delinquency",
        "children's television workshop",
        "missing children",
    }

    CATCHALL_KEYWORDS = {
        classifier.Adventure: match_kw(
            "adventure",
            "adventurers",
            "adventure stories",
            "adventure fiction",
            "adventurers",
            Eg("sea stories"),
            Eg("war stories"),
            Eg("men's adventure"),
        ),
        classifier.African_History: match_kw(
            "african history",
            "history.*africa",
        ),
        classifier.Ancient_History: match_kw(
            "ancient.*history",
            "history.*ancient",
            "civilization, classical",
        ),
        classifier.Antiques_Collectibles: match_kw(
            "antiques",
            "collectibles",
            "collectors",
            "collecting",
        ),
        classifier.Architecture: match_kw(
            "architecture",
            "architectural",
            "architect",
            "architects",
        ),
        classifier.Art: match_kw(
            "art",
            "arts",
            "artist",
            "artists",
            "artistic",
        ),
        classifier.Art_Criticism_Theory: match_kw(
            "art criticism",
            "art / criticism & theory",
        ),
        classifier.Art_History: match_kw(
            "art.*history",
        ),
        classifier.Asian_History: match_kw(
            "asian history",
            "history.*asia",
            "australasian & pacific history",
        ),
        classifier.Bartending_Cocktails: match_kw(
            "cocktail",
            "cocktails",
            "bartending",
            Eg("beer"),
            "alcoholic beverages",
            Eg("wine"),
            Eg("wine & spirits"),
            "spirits & cocktails",
        ),
        classifier.Biography_Memoir: match_kw(
            "autobiographies",
            "autobiography",
            "biographies",
            "biography",
            "biographical",
            "personal memoirs",
        ),
        classifier.Body_Mind_Spirit: match_kw(
            "body, mind & spirit",
        ),
        classifier.Buddhism: match_kw(
            "buddhism",
            "buddhist",
            "buddha",
        ),
        classifier.Business: match_kw(
            "business",
            "businesspeople",
            "businesswomen",
            "businessmen",
            "business & economics",
            "business & financial",
            "commerce",
            "sales",
            "selling",
            "sales & selling",
            Eg("nonprofit"),
        ),
        classifier.Christianity: match_kw(
            Eg("schema:creativework:bible"),
            Eg("baptist"),
            Eg("bible"),
            Eg("sermons"),
            Eg("devotional"),
            Eg("theological"),
            Eg("theology"),
            Eg("biblical"),
            "christian",
            "christianity",
            Eg("catholic"),
            Eg("protestant"),
            Eg("catholicism"),
            Eg("protestantism"),
            Eg("church"),
            Eg("christmas & advent"),
        ),
        classifier.Civil_War_History: match_kw(
            "american civil war",
            "1861-1865",
            "civil war period",
        ),
        classifier.Classics: match_kw(
            "classics",
        ),
        classifier.Computers: match_kw(
            "computer",
            "computer science",
            "computational",
            "computers",
            "computing",
            Eg("data"),
            Eg("database"),
            Eg("hardware"),
            Eg("software"),
            Eg("software development"),
            Eg("information technology"),
            Eg("web"),
            Eg("world wide web"),
        ),
        classifier.Contemporary_Romance: match_kw(
            "contemporary romance",
            "romance--contemporary",
            "romance / contemporary",
            "romance - contemporary",
        ),
        classifier.Cooking: match_kw(
            Eg("non-alcoholic"),
            Eg("baking"),
            "cookbook",
            "cooking",
            "food",
            Eg("health & healing"),
            "home economics",
            "cuisine",
        ),
        classifier.Crafts_Hobbies: match_kw(
            "arts & crafts",
            "arts, crafts",
            Eg("beadwork"),
            Eg("candle crafts"),
            Eg("candle making"),
            Eg("carving"),
            Eg("ceramics"),
            "crafts & hobbies",
            "crafts",
            Eg("crochet"),
            Eg("crocheting"),
            Eg("cross-stitch"),
            "decorative arts",
            Eg("flower arranging"),
            "folkcrafts",
            "handicrafts",
            "hobbies",
            "hobby",
            "hobbyist",
            "hobbyists",
            Eg("jewelry"),
            Eg("knitting"),
            Eg("metal work"),
            Eg("needlework"),
            Eg("origami"),
            Eg("paper crafts"),
            Eg("pottery"),
            Eg("quilting"),
            Eg("quilts"),
            Eg("scrapbooking"),
            Eg("sewing"),
            Eg("soap making"),
            Eg("stamping"),
            Eg("stenciling"),
            Eg("textile crafts"),
            Eg("toymaking"),
            Eg("weaving"),
            Eg("woodwork"),
        ),
        classifier.Design: match_kw(
            "design", "designer", "designers", Eg("graphic design"), Eg("typography")
        ),
        classifier.Dictionaries: match_kw(
            "dictionaries",
            "dictionary",
        ),
        classifier.Drama: match_kw(
            Eg("comedies"),
            "drama",
            "dramatist",
            "dramatists",
            Eg("operas"),
            Eg("plays"),
            Eg("shakespeare"),
            Eg("tragedies"),
            Eg("tragedy"),
        ),
        classifier.Economics: match_kw(
            Eg("banking"),
            "economy",
            "economies",
            "economic",
            "economics",
        ),
        classifier.Education: match_kw(
            # TODO: a lot of these don't work well because of
            # the huge amount of fiction about students. This
            # will be fixed when we institute the
            # fiction/nonfiction split.
            "education",
            "educational",
            "educator",
            "educators",
            Eg("principals"),
            "teacher",
            "teachers",
            "teaching",
            # "schools",
            # "high school",
            "schooling",
            # "student",
            # "students",
            # "college",
            Eg("university"),
            Eg("universities"),
        ),
        classifier.Epic_Fantasy: match_kw(
            "epic fantasy",
            "fantasy - epic",
            "fantasy / epic",
            "fantasy--epic",
            "fantasy/epic",
        ),
        classifier.Espionage: match_kw(
            "espionage",
            "intrigue",
            "spies",
            "spy stories",
            "spy novels",
            "spy fiction",
            "spy thriller",
        ),
        classifier.Erotica: match_kw(
            "erotic",
            "erotica",
        ),
        # TODO: history _plus_ a place
        classifier.European_History: match_kw(
            "europe.*history",
            "history.*europe",
            Eg("france.*history"),
            Eg("history.*france"),
            Eg("england.*history"),
            Eg("history.*england"),
            Eg("ireland.*history"),
            Eg("history.*ireland"),
            Eg("germany.*history"),
            Eg("history.*germany"),
            # etc. etc. etc.
        ),
        classifier.Family_Relationships: match_kw(
            "family & relationships",
            "relationships",
            "family relationships",
            "human sexuality",
            "sexuality",
        ),
        classifier.Fantasy: match_kw(
            "fantasy",
            Eg("magic"),
            Eg("wizards"),
            Eg("fairies"),
            Eg("witches"),
            Eg("dragons"),
            Eg("sorcery"),
            Eg("witchcraft"),
            Eg("wizardry"),
            Eg("unicorns"),
        ),
        classifier.Fashion: match_kw(
            "fashion",
            "fashion design",
            "fashion designers",
        ),
        classifier.Film_TV: match_kw(
            Eg("director"),
            Eg("directors"),
            "film",
            "films",
            "movies",
            "movie",
            "motion picture",
            "motion pictures",
            "moviemaker",
            "moviemakers",
            Eg("producer"),
            Eg("producers"),
            "television",
            "tv",
            "video",
        ),
        classifier.Foreign_Language_Study: match_kw(
            Eg("english as a foreign language"),
            Eg("english as a second language"),
            Eg("esl"),
            "foreign language study",
            Eg("multi-language dictionaries"),
        ),
        classifier.Games: match_kw(
            "games",
            Eg("video games"),
            "gaming",
            Eg("gambling"),
        ),
        classifier.Gardening: match_kw(
            "gardening",
            "horticulture",
        ),
        classifier.Comics_Graphic_Novels: match_kw(
            "comics",
            "comic strip",
            "comic strips",
            "comic book",
            "comic books",
            "graphic novel",
            "graphic novels",
            # Formerly in 'Superhero'
            Eg("superhero"),
            Eg("superheroes"),
            # Formerly in 'Manga'
            Eg("japanese comic books"),
            Eg("japanese comics"),
            Eg("manga"),
            Eg("yaoi"),
        ),
        classifier.Hard_Boiled_Mystery: match_kw(
            "hard-boiled",
            "noir",
        ),
        classifier.Health_Diet: match_kw(
            # ! "health services" ?
            "fitness",
            "health",
            "health aspects",
            "health & fitness",
            "hygiene",
            "nutrition",
            "diet",
            "diets",
            "weight loss",
        ),
        classifier.Hinduism: match_kw(
            "hinduism",
            "hindu",
            "hindus",
        ),
        classifier.Historical_Fiction: match_kw(
            "historical fiction",
            "fiction.*historical",
            "^historical$",
        ),
        classifier.Historical_Romance: match_kw(
            "historical romance",
            Eg("regency romance"),
            Eg("romance.*regency"),
        ),
        classifier.History: match_kw(
            "histories",
            "history",
            "historiography",
            "historical period",
            Eg("pre-confederation"),
        ),
        classifier.Horror: match_kw(
            "horror",
            Eg("occult"),
            Eg("ghost"),
            Eg("ghost stories"),
            Eg("vampires"),
            Eg("paranormal fiction"),
            Eg("occult fiction"),
            Eg("supernatural"),
            "scary",
        ),
        classifier.House_Home: match_kw(
            "house and home",
            "house & home",
            Eg("remodeling"),
            Eg("renovation"),
            Eg("caretaking"),
            Eg("interior decorating"),
        ),
        classifier.Humorous_Fiction: match_kw(
            "comedy",
            "funny",
            "humor",
            "humorous",
            "humourous",
            "humour",
            Eg("satire"),
            "wit",
        ),
        classifier.Humorous_Nonfiction: match_kw(
            "comedy",
            "funny",
            "humor",
            "humorous",
            "humour",
            "humourous",
            "wit",
        ),
        classifier.Entertainment: match_kw(
            # Almost a pure top-level category
            "entertainment",
        ),
        # These might be a problem because they might pick up
        # hateful books. Not sure if this will be a problem.
        classifier.Islam: match_kw(
            "islam",
            "islamic",
            "muslim",
            "muslims",
            Eg("halal"),
            "islamic studies",
        ),
        classifier.Judaism: match_kw(
            "judaism",
            "jewish",
            Eg("kosher"),
            "jews",
            "jewish studies",
        ),
        classifier.LGBTQ_Fiction: match_kw(
            "lgbt",
            "lgbtq",
            Eg("lesbian"),
            Eg("lesbians"),
            "gay",
            Eg("bisexual"),
            Eg("transgender"),
            Eg("transsexual"),
            Eg("transsexuals"),
            "homosexual",
            "homosexuals",
            "homosexuality",
            "queer",
        ),
        classifier.Latin_American_History: match_kw(),
        classifier.Law: match_kw(
            "court",
            "judicial",
            "law",
            "laws",
            "legislation",
            "legal",
        ),
        classifier.Legal_Thriller: match_kw(
            "legal thriller",
            "legal thrillers",
        ),
        classifier.Literary_Criticism: match_kw(
            "criticism, interpretation",
        ),
        classifier.Literary_Fiction: match_kw(
            "literary",
            "literary fiction",
            "general fiction",
            "fiction[^a-z]+general",
            "fiction[^a-z]+literary",
        ),
        classifier.Management_Leadership: match_kw(
            "management",
            "business & economics / leadership",
            "business & economics -- leadership",
            "management science",
        ),
        classifier.Mathematics: match_kw(
            Eg("algebra"),
            Eg("arithmetic"),
            Eg("calculus"),
            Eg("chaos theory"),
            Eg("game theory"),
            Eg("geometry"),
            Eg("group theory"),
            Eg("logic"),
            "math",
            "mathematical",
            "mathematician",
            "mathematicians",
            "mathematics",
            Eg("probability"),
            Eg("statistical"),
            Eg("statistics"),
            Eg("trigonometry"),
        ),
        classifier.Medical: match_kw(
            Eg("anatomy"),
            Eg("disease"),
            Eg("diseases"),
            Eg("disorders"),
            Eg("epidemiology"),
            Eg("illness"),
            Eg("illnesses"),
            "medical",
            "medicine",
            Eg("neuroscience"),
            Eg("ophthalmology"),
            Eg("physiology"),
            Eg("vaccines"),
            Eg("virus"),
        ),
        classifier.Medieval_History: match_kw(
            "civilization, medieval",
            "medieval period",
            "history.*medieval",
        ),
        classifier.Middle_East_History: match_kw(
            "middle east.*history",
            "history.*middle east",
        ),
        classifier.Military_History: match_kw(
            "military science",
            "warfare",
            "military",
            Eg("1914-1918"),
            Eg("1939-1945"),
            Eg("world war"),
        ),
        classifier.Modern_History: match_kw(
            Eg("1900 - 1999"),
            Eg("2000-2099"),
            "modern history",
            "history, modern",
            "history (modern)",
            "history--modern",
            Eg("history.*20th century"),
            Eg("history.*21st century"),
        ),
        # This is SF movie tie-ins, not movies & gaming per se.
        # This one is difficult because it takes effect if book
        # has subject "media tie-in" *and* "science fiction" or
        # "fantasy"
        classifier.Media_Tie_in_SF: match_kw(
            "science fiction & fantasy gaming",
            Eg("star trek"),
            Eg("star wars"),
            Eg("jedi"),
        ),
        classifier.Music: match_kw(
            "music",
            "musician",
            "musicians",
            "musical",
            Eg("genres & styles"),
            Eg("blues"),
            Eg("jazz"),
            Eg("rap"),
            Eg("hip-hop"),
            Eg("rock.*roll"),
            Eg("rock music"),
            Eg("punk rock"),
        ),
        classifier.Mystery: match_kw(
            Eg("crime"),
            Eg("detective"),
            Eg("murder"),
            "mystery",
            "mysteries",
            Eg("private investigators"),
            Eg("holmes, sherlock"),
            Eg("poirot, hercule"),
            Eg("schema:person:holmes, sherlock"),
        ),
        classifier.Nature: match_kw(
            # TODO: not sure about this one
            "nature",
        ),
        classifier.Body_Mind_Spirit: match_kw(
            "new age",
        ),
        classifier.Paranormal_Romance: match_kw(
            "paranormal romance",
            "romance.*paranormal",
        ),
        classifier.Parenting: match_kw(
            # "children" isn't here because the vast majority of
            # "children" tags indicate books _for_ children.
            # "family" isn't here because the vast majority
            # of "family" tags deal with specific families, e.g.
            # the Kennedys.
            "parenting",
            "parent",
            "parents",
            Eg("motherhood"),
            Eg("fatherhood"),
        ),
        classifier.Parenting_Family: match_kw(
            # Pure top-level category
        ),
        classifier.Performing_Arts: match_kw(
            "theatre",
            "theatrical",
            "performing arts",
            "entertainers",
            Eg("farce"),
            Eg("tragicomedy"),
        ),
        classifier.Periodicals: match_kw(
            "periodicals",
            "periodical",
        ),
        classifier.Personal_Finance_Investing: match_kw(
            "personal finance",
            "financial planning",
            "investing",
            Eg("retirement planning"),
            "money management",
        ),
        classifier.Pets: match_kw(
            "pets",
            Eg("dogs"),
            Eg("cats"),
        ),
        classifier.Philosophy: match_kw(
            "philosophy",
            "philosophical",
            "philosopher",
            "philosophers",
            Eg("epistemology"),
            Eg("metaphysics"),
        ),
        classifier.Photography: match_kw(
            "photography",
            "photographer",
            "photographers",
            "photographic",
        ),
        classifier.Police_Procedural: match_kw(
            "police[^a-z]+procedural",
            "police[^a-z]+procedurals",
        ),
        classifier.Poetry: match_kw(
            "poetry",
            "poet",
            "poets",
            "poem",
            "poems",
            Eg("sonnet"),
            Eg("sonnets"),
        ),
        classifier.Political_Science: match_kw(
            Eg("american government"),
            Eg("anarchism"),
            Eg("censorship"),
            Eg("citizenship"),
            Eg("civics"),
            Eg("communism"),
            Eg("corruption"),
            Eg("corrupt practices"),
            Eg("democracy"),
            Eg("geopolitics"),
            "government",
            Eg("human rights"),
            Eg("international relations"),
            Eg("political economy"),
            "political ideologies",
            "political process",
            "political science",
            Eg("public affairs"),
            Eg("public policy"),
            "politics",
            "political",
            Eg("current events"),
        ),
        classifier.Psychology: match_kw(
            "psychology",
            Eg("psychiatry"),
            "psychological aspects",
            Eg("psychiatric"),
            Eg("psychoanalysis"),
        ),
        classifier.Real_Estate: match_kw(
            "real estate",
        ),
        classifier.Reference_Study_Aids: match_kw(
            Eg("catalogs"),
            Eg("handbooks"),
            Eg("manuals"),
            Eg("reference"),
            # Formerly in 'Encyclopedias'
            Eg("encyclopaedias"),
            Eg("encyclopaedia"),
            Eg("encyclopedias"),
            Eg("encyclopedia"),
            # Formerly in 'Language Arts & Disciplines'
            Eg("alphabets"),
            Eg("communication studies"),
            Eg("composition"),
            Eg("creative writing"),
            Eg("grammar"),
            Eg("handwriting"),
            Eg("information sciences"),
            Eg("journalism"),
            Eg("library & information sciences"),
            Eg("linguistics"),
            Eg("literacy"),
            Eg("public speaking"),
            Eg("rhetoric"),
            Eg("sign language"),
            Eg("speech"),
            Eg("spelling"),
            Eg("style manuals"),
            Eg("syntax"),
            Eg("vocabulary"),
            Eg("writing systems"),
        ),
        classifier.Religion_Spirituality: match_kw(
            "religion",
            "religious",
            Eg("taoism"),
            Eg("taoist"),
            Eg("confucianism"),
            Eg("inspirational nonfiction"),
        ),
        classifier.Renaissance_Early_Modern_History: match_kw(
            "early modern period",
            "early modern history",
            "early modern, 1500-1700",
            "history.*early modern",
            "renaissance.*history",
            "history.*renaissance",
        ),
        classifier.Romance: match_kw(
            "love stories",
            "romance",
            "love & romance",
            "romances",
        ),
        classifier.Science: match_kw(
            Eg("aeronautics"),
            Eg("astronomy"),
            Eg("biology"),
            Eg("biophysics"),
            Eg("biochemistry"),
            Eg("botany"),
            Eg("chemistry"),
            Eg("earth sciences"),
            Eg("ecology"),
            Eg("entomology"),
            Eg("evolution"),
            Eg("geology"),
            Eg("genetics"),
            Eg("genetic engineering"),
            Eg("genomics"),
            Eg("ichthyology"),
            Eg("herpetology"),
            Eg("life sciences"),
            Eg("microbiology"),
            Eg("microscopy"),
            Eg("mycology"),
            Eg("ornithology"),
            Eg("natural history"),
            Eg("natural history"),
            Eg("physics"),
            "science",
            "scientist",
            "scientists",
            Eg("zoology"),
            Eg("virology"),
            Eg("cytology"),
        ),
        classifier.Science_Fiction: match_kw(
            "speculative fiction",
            "sci-fi",
            "sci fi",
            Eg("time travel"),
        ),
        # classifier.Science_Fiction_Fantasy: match_kw(
        #    "science fiction.*fantasy",
        # ),
        classifier.Self_Help: match_kw(
            "self help",
            "self-help",
            "self improvement",
            "self-improvement",
        ),
        classifier.Folklore: match_kw(
            "fables",
            "folklore",
            "folktales",
            "folk tales",
            "myth",
            "legends",
        ),
        classifier.Short_Stories: match_kw(
            "short stories",
            Eg("literary collections"),
        ),
        classifier.Social_Sciences: match_kw(
            Eg("anthropology"),
            Eg("archaeology"),
            Eg("sociology"),
            Eg("ethnic studies"),
            Eg("feminism & feminist theory"),
            Eg("gender studies"),
            Eg("media studies"),
            Eg("minority studies"),
            Eg("men's studies"),
            Eg("regional studies"),
            Eg("women's studies"),
            Eg("demography"),
            Eg("lesbian studies"),
            Eg("gay studies"),
            Eg("black studies"),
            Eg("african-american studies"),
            Eg("customs & traditions"),
            Eg("criminology"),
        ),
        classifier.Sports: match_kw(
            # Ton of specific sports here since 'players'
            # doesn't work. TODO: Why? I don't remember.
            "sports",
            Eg("baseball"),
            Eg("football"),
            Eg("hockey"),
            Eg("soccer"),
            Eg("skating"),
        ),
        classifier.Study_Aids: match_kw(
            Eg("act"),
            Eg("advanced placement"),
            Eg("bar exam"),
            Eg("clep"),
            Eg("college entrance"),
            Eg("college guides"),
            Eg("financial aid"),
            Eg("certification"),
            Eg("ged"),
            Eg("gmat"),
            Eg("gre"),
            Eg("lsat"),
            Eg("mat"),
            Eg("mcat"),
            Eg("nmsqt"),
            Eg("nte"),
            Eg("psat"),
            Eg("sat"),
            "school guides",
            "study guide",
            "study guides",
            "study aids",
            Eg("toefl"),
            "workbooks",
        ),
        classifier.Romantic_Suspense: match_kw(
            "romantic.*suspense",
            "suspense.*romance",
            "romance.*suspense",
            "romantic.*thriller",
            "romance.*thriller",
            "thriller.*romance",
        ),
        classifier.Technology: match_kw(
            "technology",
            Eg("engineering"),
            Eg("bioengineering"),
            Eg("mechanics"),
            # Formerly in 'Transportation'
            Eg("transportation"),
            Eg("railroads"),
            Eg("trains"),
            Eg("automotive"),
            Eg("ships & shipbuilding"),
            Eg("cars & trucks"),
        ),
        classifier.Suspense_Thriller: match_kw(
            "thriller",
            "thrillers",
            "suspense",
        ),
        classifier.Technothriller: match_kw(
            "techno-thriller",
            "technothriller",
            "technothrillers",
        ),
        classifier.Travel: match_kw(
            Eg("discovery"),
            "exploration",
            "travel",
            "travels.*voyages",
            "voyage.*travels",
            "voyages",
            "travelers",
            "description.*travel",
        ),
        classifier.United_States_History: match_kw(
            "united states history",
            "u.s. history",
            Eg("american revolution"),
            Eg("1775-1783"),
            Eg("revolutionary period"),
        ),
        classifier.Urban_Fantasy: match_kw(
            "urban fantasy",
            "fantasy.*urban",
        ),
        classifier.Urban_Fiction: match_kw(
            "urban fiction",
            Eg("fiction.*african american.*urban"),
        ),
        classifier.Vegetarian_Vegan: match_kw(
            "vegetarian",
            Eg("vegan"),
            Eg("veganism"),
            "vegetarianism",
        ),
        classifier.Westerns: match_kw(
            "western stories",
            "westerns",
        ),
        classifier.Women_Detectives: match_kw(
            "women detectives",
            "women detective",
            "women private investigators",
            "women private investigator",
            "women sleuths",
            "women sleuth",
        ),
        classifier.Womens_Fiction: match_kw(
            "contemporary women",
            "chick lit",
            "womens fiction",
            "women's fiction",
        ),
        classifier.World_History: match_kw(
            "world history",
            "history[^a-z]*world",
        ),
    }

    LEVEL_2_KEYWORDS = {
        classifier.Reference_Study_Aids: match_kw(
            # Formerly in 'Language Arts & Disciplines'
            Eg("language arts & disciplines"),
            Eg("language arts and disciplines"),
            Eg("language arts"),
        ),
        classifier.Design: match_kw(
            "arts and crafts movement",
        ),
        classifier.Drama: match_kw(
            Eg("opera"),
        ),
        classifier.Erotica: match_kw(
            Eg("erotic poetry"),
            Eg("gay erotica"),
            Eg("lesbian erotica"),
            Eg("erotic photography"),
        ),
        classifier.Games: match_kw(Eg("games.*fantasy")),
        classifier.Historical_Fiction: match_kw(
            Eg("arthurian romance.*"),  # This is "romance" in the old
            # sense of a story.
        ),
        classifier.Literary_Criticism: match_kw(
            Eg("literary history"),  # Not History
            Eg("romance language"),  # Not Romance
        ),
        classifier.Media_Tie_in_SF: match_kw(
            "tv, movie, video game adaptations"
        ),  # Not Film & TV
        # We need to match these first so that the 'military'/'warfare'
        # part doesn't match Military History.
        classifier.Military_SF: match_kw(
            "science fiction.*military",
            "military.*science fiction",
            Eg("space warfare"),  # Thankfully
            Eg("interstellar warfare"),
        ),
        classifier.Military_Thriller: match_kw(
            "military thrillers",
            "thrillers.*military",
        ),
        classifier.Pets: match_kw(
            "human-animal relationships",
        ),
        classifier.Political_Science: match_kw(
            Eg("health care reform"),
        ),
        # Stop the 'religious' from matching Religion/Spirituality.
        classifier.Religious_Fiction: match_kw(
            Eg("christian fiction"),
            Eg("inspirational fiction"),
            Eg("fiction.*christian"),
            "religious fiction",
            "fiction.*religious",
            Eg("Oriental religions and wisdom"),
        ),
        classifier.Romantic_Suspense: match_kw(
            "romantic.*suspense",
            "suspense.*romance",
            "romance.*suspense",
            "romantic.*thriller",
            "romance.*thriller",
            "thriller.*romance",
        ),
        # Stop from showing up as 'science'
        classifier.Social_Sciences: match_kw(
            "social sciences",
            "social science",
            "human science",
        ),
        classifier.Science_Fiction: match_kw(
            "science fiction",
            "science fiction.*general",
        ),
        classifier.Supernatural_Thriller: match_kw(
            "thriller.*supernatural",
            "supernatural.*thriller",
        ),
        # Stop from going into Mystery due to 'crime'
        classifier.True_Crime: match_kw(
            "true crime",
        ),
        # Otherwise fiction.*urban turns Urban Fantasy into Urban Fiction
        classifier.Urban_Fantasy: match_kw(
            "fiction.*fantasy.*urban",
        ),
        # Stop the 'children' in 'children of' from matching Parenting.
        None: match_kw(
            "children of",
        ),
    }

    LEVEL_3_KEYWORDS = {
        classifier.Space_Opera: match_kw(
            "space opera",
        ),
    }

    @classmethod
    def is_fiction(cls, identifier, name, exclude_examples=False):
        if not name:
            return None
        if cls.LEVEL_1_NONFICTION_INDICATORS["search"](name, exclude_examples):
            return False
        if cls.LEVEL_2_FICTION_INDICATORS["search"](name, exclude_examples):
            return True
        if cls.LEVEL_2_NONFICTION_INDICATORS["search"](name, exclude_examples):
            return False
        return None

    @classmethod
    def audience(cls, identifier, name, exclude_examples=False):
        if name is None:
            return None
        if cls.YOUNG_ADULT_INDICATORS["search"](name, exclude_examples):
            use = cls.AUDIENCE_YOUNG_ADULT
        elif cls.JUVENILE_INDICATORS["search"](name, exclude_examples):
            use = cls.AUDIENCE_CHILDREN
        else:
            return None

        if use == cls.AUDIENCE_CHILDREN:
            for i in cls.JUVENILE_TERMS_THAT_IMPLY_YOUNG_ADULT:
                if i in name:
                    use = cls.AUDIENCE_YOUNG_ADULT

        # It may be for kids, or it may be about kids, e.g. "juvenile
        # delinquency".
        for i in cls.JUVENILE_BLACKLIST:
            if i in name:
                return None
        return use

    @classmethod
    def audience_match(cls, query):
        audience = None
        audience_words = None
        audience = cls.audience(None, query, exclude_examples=True)
        if audience:
            for audience_keywords in [
                cls.JUVENILE_INDICATORS,
                cls.YOUNG_ADULT_INDICATORS,
            ]:
                match = audience_keywords["search"](query, exclude_examples=True)
                if match:
                    audience_words = match.group()
                    break
        return (audience, audience_words)

    @classmethod
    def genre(
        cls, identifier, name, fiction=None, audience=None, exclude_examples=False
    ):
        matches = Counter()
        match_against = [name]
        for l in [cls.LEVEL_3_KEYWORDS, cls.LEVEL_2_KEYWORDS, cls.CATCHALL_KEYWORDS]:
            for genre, keywords in list(l.items()):
                if genre and fiction is not None and genre.is_fiction != fiction:
                    continue
                if (
                    genre
                    and audience
                    and genre.audience_restriction
                    and audience not in genre.audience_restriction
                ):
                    continue
                if keywords and keywords["search"](name, exclude_examples):
                    matches[genre] += 1
            most_specific_genre = None
            most_specific_count = 0
            # The genre with the most regex matches wins.
            #
            # If a genre and a subgenre are tied, then the subgenre wins
            # because it's more specific.
            for genre, count in matches.most_common():
                if not most_specific_genre or (
                    most_specific_genre.has_subgenre(genre)
                    and count >= most_specific_count
                ):
                    most_specific_genre = genre
                    most_specific_count = count
            if most_specific_genre:
                break
        return most_specific_genre

    @classmethod
    def genre_match(cls, query):
        genre = None
        genre_words = None
        genre = cls.genre(None, query, exclude_examples=True)
        if genre:
            for kwlist in [
                cls.LEVEL_3_KEYWORDS,
                cls.LEVEL_2_KEYWORDS,
                cls.CATCHALL_KEYWORDS,
            ]:
                if genre in list(kwlist.keys()):
                    genre_keywords = kwlist[genre]
                    match = genre_keywords["search"](query, exclude_examples=True)
                    if match:
                        genre_words = match.group()
                        break
        return (genre, genre_words)


class LCSHClassifier(KeywordBasedClassifier):
    pass


class FASTClassifier(KeywordBasedClassifier):
    pass


class TAGClassifier(KeywordBasedClassifier):
    pass
