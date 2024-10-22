from urllib.parse import unquote

from palace.manager.core import classifier
from palace.manager.core.classifier import NO_VALUE, Classifier, Lowercased


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
            all_genres = classifier.fiction_genres
        elif fiction == False:
            all_genres = classifier.nonfiction_genres
        else:
            all_genres = classifier.fiction_genres + classifier.nonfiction_genres
        return cls._genre_by_name(identifier.original, all_genres)

    @classmethod
    def is_fiction(cls, identifier, name):
        if not classifier.genres.get(identifier.original):
            return None
        return classifier.genres[identifier.original].is_fiction

    @classmethod
    def _genre_by_name(cls, name, genres):
        for genre in genres:
            if genre == name:
                return classifier.genres[name]
            elif isinstance(genre, dict):
                if name == genre["name"] or name in genre.get("subgenres", []):
                    return classifier.genres[name]
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
