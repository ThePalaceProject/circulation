from __future__ import annotations

import logging
from collections import Counter

from sqlalchemy.orm import Session

from palace.manager.core import classifier
from palace.manager.core.classifier import Classifier, GenreData, genres
from palace.manager.core.classifier.simplified import SimplifiedGenreClassifier


class WorkClassifier:
    """Boil down a bunch of Classification objects into a few values."""

    # TODO: This needs a lot of additions.
    genre_publishers = {
        "Harlequin": classifier.Romance,
        "Pocket Books/Star Trek": classifier.Media_Tie_in_SF,
        "Kensington": classifier.Urban_Fiction,
        "Fodor's Travel Publications": classifier.Travel,
        "Marvel Entertainment, LLC": classifier.Comics_Graphic_Novels,
    }

    genre_imprints = {
        "Harlequin Intrigue": classifier.Romantic_Suspense,
        "Love Inspired Suspense": classifier.Romantic_Suspense,
        "Harlequin Historical": classifier.Historical_Romance,
        "Harlequin Historical Undone": classifier.Historical_Romance,
        "Frommers": classifier.Travel,
        "LucasBooks": classifier.Media_Tie_in_SF,
    }

    audience_imprints = {
        "Harlequin Teen": Classifier.AUDIENCE_YOUNG_ADULT,
        "HarperTeen": Classifier.AUDIENCE_YOUNG_ADULT,
        "Open Road Media Teen & Tween": Classifier.AUDIENCE_YOUNG_ADULT,
        "Rosen Young Adult": Classifier.AUDIENCE_YOUNG_ADULT,
    }

    not_adult_publishers = {
        "Scholastic Inc.",
        "Random House Children's Books",
        "Little, Brown Books for Young Readers",
        "Penguin Young Readers Group",
        "Hachette Children's Books",
        "Nickelodeon Publishing",
    }

    not_adult_imprints = {
        "Scholastic",
        "Scholastic Paperbacks",
        "Random House Books for Young Readers",
        "HMH Books for Young Readers",
        "Knopf Books for Young Readers",
        "Delacorte Books for Young Readers",
        "Open Road Media Young Readers",
        "Macmillan Young Listeners",
        "Bloomsbury Childrens",
        "NYR Children's Collection",
        "Bloomsbury USA Childrens",
        "National Geographic Children's Books",
    }

    fiction_imprints = {"Del Rey"}
    nonfiction_imprints = {"Harlequin Nonfiction"}

    nonfiction_publishers = {"Wiley"}
    fiction_publishers: set[str] = set()

    def __init__(self, work, test_session=None, debug=False):
        self._db = Session.object_session(work)
        if test_session:
            self._db = test_session
        self.work = work
        self.fiction_weights = Counter()
        self.audience_weights = Counter()
        self.target_age_lower_weights = Counter()
        self.target_age_upper_weights = Counter()
        self.genre_weights = Counter()
        self.direct_from_license_source = set()
        self.prepared = False
        self.debug = debug
        self.classifications = []
        self.seen_classifications = set()
        self.log = logging.getLogger("Classifier (workid=%d)" % self.work.id)
        self.using_staff_genres = False
        self.using_staff_fiction_status = False
        self.using_staff_audience = False
        self.using_staff_target_age = False

        # Keep track of whether we've seen one of Overdrive's generic
        # "Juvenile" classifications, as well as its more specific
        # subsets like "Picture Books" and "Beginning Readers"
        self.overdrive_juvenile_generic = False
        self.overdrive_juvenile_with_target_age = False

    def add(self, classification):
        """Prepare a single Classification for consideration."""

        # We only consider a given classification once from a given
        # data source.
        key = (classification.subject, classification.data_source)
        if key in self.seen_classifications:
            return
        self.seen_classifications.add(key)
        if self.debug:
            self.classifications.append(classification)

        # Make sure the Subject is ready to be used in calculations.
        if not classification.subject.checked:  # or self.debug
            classification.subject.assign_to_genre()

        if classification.comes_from_license_source:
            self.direct_from_license_source.add(classification)
        else:
            if classification.subject.describes_format:
                # TODO: This is a bit of a hack.
                #
                # Only accept a classification having to do with
                # format (e.g. 'comic books') if that classification
                # comes direct from the license source. Otherwise it's
                # really easy for a graphic adaptation of a novel to
                # get mixed up with the original novel, whereupon the
                # original book is classified as a graphic novel.
                return

        # Put the weight of the classification behind various
        # considerations.
        weight = classification.scaled_weight
        subject = classification.subject
        from palace.manager.sqlalchemy.model.datasource import DataSource

        from_staff = classification.data_source.name == DataSource.LIBRARY_STAFF

        # if classification is genre or NONE from staff, ignore all non-staff genres
        is_genre = subject.genre != None
        from palace.manager.sqlalchemy.model.classification import Subject

        is_none = (
            from_staff
            and subject.type == Subject.SIMPLIFIED_GENRE
            and subject.identifier == SimplifiedGenreClassifier.NONE
        )
        if is_genre or is_none:
            if not from_staff and self.using_staff_genres:
                return
            if from_staff and not self.using_staff_genres:
                # first encounter with staff genre, so throw out existing genre weights
                self.using_staff_genres = True
                self.genre_weights = Counter()
            if is_genre:
                self.weigh_genre(subject.genre, weight)

        # if staff classification is fiction or nonfiction, ignore all other fictions
        if not self.using_staff_fiction_status:
            if from_staff and subject.type == Subject.SIMPLIFIED_FICTION_STATUS:
                # encountering first staff fiction status,
                # so throw out existing fiction weights
                self.using_staff_fiction_status = True
                self.fiction_weights = Counter()
            self.fiction_weights[subject.fiction] += weight

        # if staff classification is about audience, ignore all other audience classifications
        if not self.using_staff_audience:
            if from_staff and subject.type == Subject.FREEFORM_AUDIENCE:
                self.using_staff_audience = True
                self.audience_weights = Counter()
                self.audience_weights[subject.audience] += weight
            else:
                if classification.generic_juvenile_audience:
                    # We have a generic 'juvenile' classification. The
                    # audience might say 'Children' or it might say 'Young
                    # Adult' but we don't actually know which it is.
                    #
                    # We're going to split the difference, with a slight
                    # preference for YA, to bias against showing
                    # age-inappropriate material to children. To
                    # counterbalance the fact that we're splitting up the
                    # weight this way, we're also going to treat this
                    # classification as evidence _against_ an 'adult'
                    # classification.
                    self.audience_weights[Classifier.AUDIENCE_YOUNG_ADULT] += (
                        weight * 0.6
                    )
                    self.audience_weights[Classifier.AUDIENCE_CHILDREN] += weight * 0.4
                    for audience in Classifier.AUDIENCES_ADULT:
                        if audience != Classifier.AUDIENCE_ALL_AGES:
                            # 'All Ages' is considered an adult audience,
                            # but a generic 'juvenile' classification
                            # is not evidence against it.
                            self.audience_weights[audience] -= weight * 0.5
                else:
                    self.audience_weights[subject.audience] += weight

        if not self.using_staff_target_age:
            if from_staff and subject.type == Subject.AGE_RANGE:
                self.using_staff_target_age = True
                self.target_age_lower_weights = Counter()
                self.target_age_upper_weights = Counter()
            if subject.target_age:
                # Figure out how reliable this classification really is as
                # an indicator of a target age.
                scaled_weight = classification.weight_as_indicator_of_target_age
                target_min = subject.target_age.lower
                target_max = subject.target_age.upper
                if target_min is not None:
                    if not subject.target_age.lower_inc:
                        target_min += 1
                    self.target_age_lower_weights[target_min] += scaled_weight
                if target_max is not None:
                    if not subject.target_age.upper_inc:
                        target_max -= 1
                    self.target_age_upper_weights[target_max] += scaled_weight

        if not self.using_staff_audience and not self.using_staff_target_age:
            if (
                subject.type == "Overdrive"
                and subject.audience == Classifier.AUDIENCE_CHILDREN
            ):
                if subject.target_age and (
                    subject.target_age.lower or subject.target_age.upper
                ):
                    # This is a juvenile classification like "Picture
                    # Books" which implies a target age.
                    self.overdrive_juvenile_with_target_age = classification
                else:
                    # This is a generic juvenile classification like
                    # "Juvenile Fiction".
                    self.overdrive_juvenile_generic = classification

    def weigh_metadata(self):
        """Modify the weights according to the given Work's metadata.

        Use work metadata to simulate classifications.

        This is basic stuff, like: Harlequin tends to publish
        romances.
        """
        if self.work.title and (
            "Star Trek:" in self.work.title
            or "Star Wars:" in self.work.title
            or ("Jedi" in self.work.title and self.work.imprint == "Del Rey")
        ):
            self.weigh_genre(classifier.Media_Tie_in_SF, 100)

        publisher = self.work.publisher
        imprint = self.work.imprint
        if (
            imprint in self.nonfiction_imprints
            or publisher in self.nonfiction_publishers
        ):
            self.fiction_weights[False] = 100
        elif imprint in self.fiction_imprints or publisher in self.fiction_publishers:
            self.fiction_weights[True] = 100

        if imprint in self.genre_imprints:
            self.weigh_genre(self.genre_imprints[imprint], 100)
        elif publisher in self.genre_publishers:
            self.weigh_genre(self.genre_publishers[publisher], 100)

        if imprint in self.audience_imprints:
            self.audience_weights[self.audience_imprints[imprint]] += 100
        elif (
            publisher in self.not_adult_publishers or imprint in self.not_adult_imprints
        ):
            for audience in [
                Classifier.AUDIENCE_ADULT,
                Classifier.AUDIENCE_ADULTS_ONLY,
            ]:
                self.audience_weights[audience] -= 100

    def prepare_to_classify(self):
        """Called the first time classify() is called. Does miscellaneous
        one-time prep work that requires all data to be in place.
        """
        self.weigh_metadata()

        explicitly_indicated_audiences = (
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_YOUNG_ADULT,
            Classifier.AUDIENCE_ADULTS_ONLY,
        )
        audiences_from_license_source = {
            classification.subject.audience
            for classification in self.direct_from_license_source
        }
        if (
            self.direct_from_license_source
            and not self.using_staff_audience
            and not any(
                audience in explicitly_indicated_audiences
                for audience in audiences_from_license_source
            )
        ):
            # If this was erotica, or a book for children or young
            # adults, the distributor would have given some indication
            # of that fact. In the absense of any such indication, we
            # can assume very strongly that this is a regular old book
            # for adults.
            #
            # 3M is terrible at distinguishing between childrens'
            # books and YA books, but books for adults can be
            # distinguished by their _lack_ of childrens/YA
            # classifications.
            self.audience_weights[Classifier.AUDIENCE_ADULT] += 500

        if (
            self.overdrive_juvenile_generic
            and not self.overdrive_juvenile_with_target_age
        ):
            # This book is classified under 'Juvenile Fiction' but not
            # under 'Picture Books' or 'Beginning Readers'. The
            # implicit target age here is 9-12 (the portion of
            # Overdrive's 'juvenile' age range not covered by 'Picture
            # Books' or 'Beginning Readers'.
            weight = self.overdrive_juvenile_generic.weight_as_indicator_of_target_age
            self.target_age_lower_weights[9] += weight
            self.target_age_upper_weights[12] += weight

        self.prepared = True

    def classify(self, default_fiction=None, default_audience=None):
        # Do a little prep work.
        if not self.prepared:
            self.prepare_to_classify()

        if self.debug:
            for c in self.classifications:
                self.log.debug(
                    "%d %r (via %s)", c.weight, c.subject, c.data_source.name
                )

        # Actually figure out the classifications
        fiction = self.fiction(default_fiction=default_fiction)
        genres = self.genres(fiction)
        audience = self.audience(genres, default_audience=default_audience)
        target_age = self.target_age(audience)
        if self.debug:
            self.log.debug("Fiction weights:")
            for k, v in self.fiction_weights.most_common():
                self.log.debug(" %s: %s", v, k)
            self.log.debug("Genre weights:")
            for k, v in self.genre_weights.most_common():
                self.log.debug(" %s: %s", v, k)
            self.log.debug("Audience weights:")
            for k, v in self.audience_weights.most_common():
                self.log.debug(" %s: %s", v, k)
        return genres, fiction, audience, target_age

    def fiction(self, default_fiction=None):
        """Is it more likely this is a fiction or nonfiction book?"""
        if not self.fiction_weights:
            # We have absolutely no idea one way or the other, and it
            # would be irresponsible to guess.
            return default_fiction
        is_fiction = default_fiction
        if self.fiction_weights[True] > self.fiction_weights[False]:
            is_fiction = True
        elif self.fiction_weights[False] > 0:
            is_fiction = False
        return is_fiction

    def audience(self, genres=[], default_audience=None):
        """What's the most likely audience for this book?
        :param default_audience: To avoid embarassing situations we will
        classify works as being intended for adults absent convincing
        evidence to the contrary. In some situations (like the metadata
        wrangler), it's better to state that we have no information, so
        default_audience can be set to None.
        """

        # If we determined that Erotica was a significant enough
        # component of the classification to count as a genre, the
        # audience will always be 'Adults Only', even if the audience
        # weights would indicate something else.
        if classifier.Erotica in genres:
            return Classifier.AUDIENCE_ADULTS_ONLY

        w = self.audience_weights
        if not self.audience_weights:
            # We have absolutely no idea, and it would be
            # irresponsible to guess.
            return default_audience

        children_weight = w.get(Classifier.AUDIENCE_CHILDREN, 0)
        ya_weight = w.get(Classifier.AUDIENCE_YOUNG_ADULT, 0)
        adult_weight = w.get(Classifier.AUDIENCE_ADULT, 0)
        adults_only_weight = w.get(Classifier.AUDIENCE_ADULTS_ONLY, 0)
        all_ages_weight = w.get(Classifier.AUDIENCE_ALL_AGES, 0)
        research_weight = w.get(Classifier.AUDIENCE_RESEARCH, 0)

        total_adult_weight = adult_weight + adults_only_weight
        total_weight = sum(w.values())

        audience = default_audience

        # A book will be classified as a young adult or childrens'
        # book when the weight of that audience is more than twice the
        # combined weight of the 'adult' and 'adults only' audiences.
        # If that combined weight is zero, then any amount of evidence
        # is sufficient.
        threshold = total_adult_weight * 2

        # If both the 'children' weight and the 'YA' weight pass the
        # threshold, we go with the one that weighs more.
        # If the 'children' weight passes the threshold on its own
        # we go with 'children'.
        total_juvenile_weight = children_weight + ya_weight
        if (
            research_weight > (total_adult_weight + all_ages_weight)
            and research_weight > (total_juvenile_weight + all_ages_weight)
            and research_weight > threshold
        ):
            audience = Classifier.AUDIENCE_RESEARCH
        elif (
            all_ages_weight > total_adult_weight
            and all_ages_weight > total_juvenile_weight
        ):
            audience = Classifier.AUDIENCE_ALL_AGES
        elif children_weight > threshold and children_weight > ya_weight:
            audience = Classifier.AUDIENCE_CHILDREN
        elif ya_weight > threshold:
            audience = Classifier.AUDIENCE_YOUNG_ADULT
        elif total_juvenile_weight > threshold:
            # Neither weight passes the threshold on its own, but
            # combined they do pass the threshold. Go with
            # 'Young Adult' to be safe.
            audience = Classifier.AUDIENCE_YOUNG_ADULT
        elif total_adult_weight > 0:
            audience = Classifier.AUDIENCE_ADULT

        # If the 'adults only' weight is more than 1/4 of the total adult
        # weight, classify as 'adults only' to be safe.
        #
        # TODO: This has not been calibrated.
        if (
            audience == Classifier.AUDIENCE_ADULT
            and adults_only_weight > total_adult_weight / 4
        ):
            audience = Classifier.AUDIENCE_ADULTS_ONLY

        return audience

    @classmethod
    def top_tier_values(self, counter):
        """Given a Counter mapping values to their frequency of occurance,
        return all values that are as common as the most common value.
        """
        top_frequency = None
        top_tier = set()
        for age, freq in counter.most_common():
            if not top_frequency:
                top_frequency = freq
            if freq != top_frequency:
                # We've run out of candidates
                break
            else:
                # This candidate occurs with the maximum frequency.
                top_tier.add(age)
        return top_tier

    def target_age(self, audience):
        """Derive a target age from the gathered data."""
        if audience not in (
            Classifier.AUDIENCE_CHILDREN,
            Classifier.AUDIENCE_YOUNG_ADULT,
        ):
            # This is not a children's or YA book. Assertions about
            # target age are irrelevant and the default value rules.
            return Classifier.default_target_age_for_audience(audience)

        # Only consider the most reliable classifications.

        # Try to reach consensus on the lower and upper bounds of the
        # age range.
        if self.debug:
            if self.target_age_lower_weights:
                self.log.debug("Possible target age minima:")
                for k, v in self.target_age_lower_weights.most_common():
                    self.log.debug(" %s: %s", v, k)
            if self.target_age_upper_weights:
                self.log.debug("Possible target age maxima:")
                for k, v in self.target_age_upper_weights.most_common():
                    self.log.debug(" %s: %s", v, k)

        target_age_min = None
        target_age_max = None
        if self.target_age_lower_weights:
            # Find the youngest age in the top tier of values.
            candidates = self.top_tier_values(self.target_age_lower_weights)
            target_age_min = min(candidates)

        if self.target_age_upper_weights:
            # Find the oldest age in the top tier of values.
            candidates = self.top_tier_values(self.target_age_upper_weights)
            target_age_max = max(candidates)

        if not target_age_min and not target_age_max:
            # We found no opinions about target age. Use the default.
            return Classifier.default_target_age_for_audience(audience)

        if target_age_min is None:
            target_age_min = target_age_max

        if target_age_max is None:
            target_age_max = target_age_min

        # Err on the side of setting the minimum age too high.
        if target_age_min > target_age_max:
            target_age_max = target_age_min
        return Classifier.range_tuple(target_age_min, target_age_max)

    def genres(self, fiction, cutoff=0.15):
        """Consolidate genres and apply a low-pass filter."""
        # Remove any genres whose fiction status is inconsistent with the
        # (independently determined) fiction status of the book.
        #
        # It doesn't matter if a book is classified as 'science
        # fiction' 100 times; if we know it's nonfiction, it can't be
        # science fiction. (It's probably a history of science fiction
        # or something.)
        genres = dict(self.genre_weights)
        if not genres:
            # We have absolutely no idea, and it would be
            # irresponsible to guess.
            return {}

        for genre in list(genres.keys()):
            # If we have a fiction determination, that lets us eliminate
            # possible genres that conflict with that determination.
            #
            # TODO: If we don't have a fiction determination, the
            # genres we end up with may help us make one.
            if fiction is not None and (genre.default_fiction != fiction):
                del genres[genre]

        # Consolidate parent genres into their heaviest subgenre.
        genres = self.consolidate_genre_weights(genres)
        total_weight = float(sum(genres.values()))

        # Strip out the stragglers.
        for g, score in list(genres.items()):
            affinity = score / total_weight
            if affinity < cutoff:
                total_weight -= score
                del genres[g]
        return genres

    def weigh_genre(self, genre_data, weight):
        """A helper method that ensure we always use database Genre
        objects, not GenreData objects, when weighting genres.
        """
        from palace.manager.sqlalchemy.model.classification import Genre

        genre, ignore = Genre.lookup(self._db, genre_data.name)
        self.genre_weights[genre] += weight

    @classmethod
    def consolidate_genre_weights(cls, weights, subgenre_swallows_parent_at=0.03):
        """If a genre and its subgenres both show up, examine the subgenre
        with the highest weight. If its weight exceeds a certain
        proportion of the weight of the parent genre, assign the
        parent's weight to the subgenre and remove the parent.
        """
        # print("Before consolidation:")
        # for genre, weight in weights.items():
        #    print("", genre, weight)

        # Convert Genre objects to GenreData.
        consolidated = Counter()
        for genre, weight in list(weights.items()):
            if not isinstance(genre, GenreData):
                genre = genres[genre.name]
            consolidated[genre] += weight

        heaviest_child = dict()
        for genre, weight in list(consolidated.items()):
            for parent in genre.parents:
                if parent in consolidated:
                    if (not parent in heaviest_child) or weight > heaviest_child[
                        parent
                    ][1]:
                        heaviest_child[parent] = (genre, weight)
        # print("Heaviest child:")
        # for parent, (genre, weight) in heaviest_child.items():
        #    print("", parent, genre, weight)
        made_it = False
        while not made_it:
            for parent, (child, weight) in sorted(
                heaviest_child.items(), key=lambda genre: genre[1][1], reverse=True
            ):
                parent_weight = consolidated.get(parent, 0)
                if weight > (subgenre_swallows_parent_at * parent_weight):
                    consolidated[child] += parent_weight
                    del consolidated[parent]
                    changed = False
                    for parent in parent.parents:
                        if parent in heaviest_child:
                            heaviest_child[parent] = (child, consolidated[child])
                            changed = True
                    if changed:
                        # We changed the dict, so we need to restart
                        # the iteration.
                        break
            # We made it all the way through the dict without changing it.
            made_it = True
        # print("Final heaviest child:")
        # for parent, (genre, weight) in heaviest_child.items():
        #    print("", parent, genre, weight)
        # print("After consolidation:")
        # for genre, weight in consolidated.items():
        #    print("", genre, weight)
        return consolidated
