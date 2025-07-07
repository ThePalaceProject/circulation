from palace.manager.core import classifier


class OverdriveClassifier(classifier.Classifier):
    # These genres are only used to describe video titles.
    VIDEO_GENRES = [
        "Action",
        "Adventure",
        "Animation",
        "Ballet",
        "Cartoon",
        "Classic Film",
        "Comedy",
        "Children's Video",
        "Documentary",
        "Feature Film",
        "Foreign Film",
        "Instructional",
        "Martial Arts",
        "Music Video",
        "Short Film",
        "Stage Production",
        "Theater",
        "TV Series",
        "Young Adult Video",
    ]

    # These genres are only used to describe music titles.
    MUSIC_GENRES = [
        "Alternative",
        "Ambient",
        "Blues",
        "Chamber Music",
        "Children's Music",
        "Choral",
        "Christian",
        "Classical",
        "Compilations",
        "Concertos",
        "Country",
        "Dance",
        "Electronica",
        "Film Music",
        "Folk",
        "Hip-Hop",
        "Holiday Music",
        "Indie",
        "Instrumental",
        "Jazz",
        "Opera & Operetta",
        "Orchestral",
        "Pop",
        "Ragtime",
        "Rap",
        "R & B",
        "Rock",
        "Soundtrack",
        "Vocal",
        "World Music",
    ]

    # Any classification that includes the string "Fiction" will be
    # counted as fiction. This is just the leftovers.
    FICTION = {
        "Fantasy",
        "Horror",
        "Literary Anthologies",
        "Mystery",
        "Romance",
        "Short Stories",
        "Suspense",
        "Thriller",
        "Western",
    }

    NEITHER_FICTION_NOR_NONFICTION = (
        [
            "Drama",
            "Poetry",
            "Latin",
        ]
        + MUSIC_GENRES
        + VIDEO_GENRES
    )

    GENRES = {
        classifier.Antiques_Collectibles: "Antiques",
        classifier.Architecture: "Architecture",
        classifier.Art: "Art",
        classifier.Biography_Memoir: "Biography & Autobiography",
        classifier.Business: ["Business", "Marketing & Sales", "Careers"],
        classifier.Christianity: "Christian Nonfiction",
        classifier.Computers: ["Computer Technology", "Social Media"],
        classifier.Classics: "Classic Literature",
        classifier.Cooking: "Cooking & Food",
        classifier.Crafts_Hobbies: "Crafts",
        classifier.Games: "Games",
        classifier.Drama: "Drama",
        classifier.Economics: "Economics",
        classifier.Education: "Education",
        classifier.Erotica: "Erotic Literature",
        classifier.Fantasy: "Fantasy",
        classifier.Folklore: ["Folklore", "Mythology"],
        classifier.Foreign_Language_Study: "Foreign Language Study",
        classifier.Gardening: "Gardening",
        classifier.Comics_Graphic_Novels: "Comic and Graphic Books",
        classifier.Health_Diet: "Health & Fitness",
        classifier.Historical_Fiction: ["Historical Fiction", "Antiquarian"],
        classifier.History: "History",
        classifier.Horror: "Horror",
        classifier.House_Home: "Home Design & Décor",
        classifier.Humorous_Fiction: "Humor (Fiction)",
        classifier.Humorous_Nonfiction: "Humor (Nonfiction)",
        classifier.Entertainment: "Entertainment",
        classifier.Judaism: "Judaica",
        classifier.Law: "Law",
        classifier.Literary_Criticism: [
            "Literary Criticism",
            "Criticism",
            "Language Arts",
            "Writing",
        ],
        classifier.Management_Leadership: "Management",
        classifier.Mathematics: "Mathematics",
        classifier.Medical: "Medical",
        classifier.Military_History: "Military",
        classifier.Music: ["Music", "Songbook"],
        classifier.Mystery: "Mystery",
        classifier.Nature: "Nature",
        classifier.Body_Mind_Spirit: "New Age",
        classifier.Parenting_Family: ["Family & Relationships", "Child Development"],
        classifier.Performing_Arts: "Performing Arts",
        classifier.Personal_Finance_Investing: "Finance",
        classifier.Pets: "Pets",
        classifier.Philosophy: ["Philosophy", "Ethics"],
        classifier.Photography: "Photography",
        classifier.Poetry: "Poetry",
        classifier.Political_Science: ["Politics", "Current Events"],
        classifier.Psychology: ["Psychology", "Psychiatry", "Psychiatry & Psychology"],
        classifier.Reference_Study_Aids: ["Reference", "Grammar & Language Usage"],
        classifier.Religious_Fiction: ["Christian Fiction"],
        classifier.Religion_Spirituality: "Religion & Spirituality",
        classifier.Romance: "Romance",
        classifier.Science: ["Science", "Physics", "Chemistry", "Biology"],
        classifier.Science_Fiction: "Science Fiction",
        # Science_Fiction_Fantasy : "Science Fiction & Fantasy",
        classifier.Self_Help: [
            "Self-Improvement",
            "Self-Help",
            "Self Help",
            "Recovery",
        ],
        classifier.Short_Stories: ["Literary Anthologies", "Short Stories"],
        classifier.Social_Sciences: [
            "Sociology",
            "Gender Studies",
            "Genealogy",
            "Media Studies",
            "Social Studies",
        ],
        classifier.Sports: "Sports & Recreations",
        classifier.Study_Aids: ["Study Aids & Workbooks", "Text Book"],
        classifier.Technology: ["Technology", "Engineering", "Transportation"],
        classifier.Suspense_Thriller: ["Suspense", "Thriller"],
        classifier.Travel: ["Travel", "Travel Literature", "Outdoor Recreation"],
        classifier.True_Crime: "True Crime",
        classifier.Urban_Fiction: ["African American Fiction", "Urban Fiction"],
        classifier.Westerns: "Western",
        classifier.Womens_Fiction: "Chick Lit Fiction",
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier
        if identifier.startswith("Foreign Language Study"):
            return "Foreign Language Study"
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        if (
            identifier in cls.FICTION
            or "Fiction" in identifier
            or "Literature" in identifier
        ):
            # "Literature" on Overdrive seems to be synonymous with fiction,
            # but not necessarily "Literary Fiction".
            return True

        if identifier in cls.NEITHER_FICTION_NOR_NONFICTION:
            return None

        # Everything else is presumed nonfiction.
        return False

    @classmethod
    def audience(cls, identifier, name):
        if (
            "Juvenile" in identifier
            or "Picture Book" in identifier
            or "Beginning Reader" in identifier
            or "Children's" in identifier
        ):
            return cls.AUDIENCE_CHILDREN
        elif "Young Adult" in identifier:
            return cls.AUDIENCE_YOUNG_ADULT
        elif identifier in ("Fiction", "Nonfiction"):
            return cls.AUDIENCE_ADULT
        elif identifier == "Erotic Literature":
            return cls.AUDIENCE_ADULTS_ONLY
        return None

    @classmethod
    def target_age(cls, identifier, name):
        if identifier.startswith("Picture Book"):
            return cls.range_tuple(0, 4)
        elif identifier.startswith("Beginning Reader"):
            return cls.range_tuple(5, 8)
        elif "Young Adult" in identifier:
            # Internally we believe that 'Young Adult' means ages
            # 14-17, but after looking at a large number of Overdrive
            # books classified as 'Young Adult' we think that
            # Overdrive means something closer to 12-17.
            return cls.range_tuple(12, 17)
        return cls.range_tuple(None, None)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for l, v in list(cls.GENRES.items()):
            if identifier == v or (isinstance(v, list) and identifier in v):
                return l
        if identifier == "Gay/Lesbian" and fiction:
            return classifier.LGBTQ_Fiction
        return None
