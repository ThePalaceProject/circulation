from urllib.parse import unquote

from palace.manager.core.classifier import (
    NO_VALUE,
    Classifier,
    Lowercased,
    fiction_genres,
    genres,
    nonfiction_genres,
)


class SimplifiedGenreClassifier(Classifier):
    NONE = NO_VALUE

    @classmethod
    def scrub_identifier(cls, identifier):
        # If the identifier is a URI identifying a Simplified genre,
        # strip off the first part of the URI to get the genre name.
        if not identifier:
            return identifier
        if identifier.startswith(cls.SIMPLIFIED_GENRE):
            identifier = identifier[len(cls.SIMPLIFIED_GENRE) :]
            identifier = unquote(identifier)
        return Lowercased(identifier)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        if fiction == True:
            all_genres = fiction_genres
        elif fiction == False:
            all_genres = nonfiction_genres
        else:
            all_genres = fiction_genres + nonfiction_genres
        return cls._genre_by_name(identifier.original, all_genres)

    @classmethod
    def is_fiction(cls, identifier, name):
        if not genres.get(identifier.original):
            return None
        return genres[identifier.original].is_fiction

    @classmethod
    def _genre_by_name(cls, name, genres):
        for genre in genres:
            if genre == name:
                return genres[name]
            elif isinstance(genre, dict):
                if name == genre["name"] or name in genre.get("subgenres", []):
                    return genres[name]
        return None


class SimplifiedFictionClassifier(Classifier):
    @classmethod
    def scrub_identifier(cls, identifier):
        # If the identifier is a URI identifying a Simplified genre,
        # strip off the first part of the URI to get the genre name.
        if not identifier:
            return identifier
        if identifier.startswith(cls.SIMPLIFIED_FICTION_STATUS):
            identifier = identifier[len(cls.SIMPLIFIED_FICTION_STATUS) :]
            identifier = unquote(identifier)
        return Lowercased(identifier)

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier == "fiction":
            return True
        elif identifier == "nonfiction":
            return False
        else:
            return None


Classifier.classifiers[Classifier.SIMPLIFIED_GENRE] = SimplifiedGenreClassifier
Classifier.classifiers[Classifier.SIMPLIFIED_FICTION_STATUS] = (
    SimplifiedFictionClassifier
)
