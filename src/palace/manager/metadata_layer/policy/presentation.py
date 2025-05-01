from __future__ import annotations

from typing_extensions import Self


class PresentationCalculationPolicy:
    """Which parts of the Work or Edition's presentation
    are we actually looking to update?
    """

    DEFAULT_LEVELS = 3
    DEFAULT_THRESHOLD = 0.5
    DEFAULT_CUTOFF = 1000

    def __init__(
        self,
        *,
        choose_edition: bool = True,
        set_edition_metadata: bool = True,
        classify: bool = True,
        choose_summary: bool = True,
        calculate_quality: bool = True,
        choose_cover: bool = True,
        update_search_index: bool = False,
        verbose: bool = True,
        equivalent_identifier_levels: int = DEFAULT_LEVELS,
        equivalent_identifier_threshold: float = DEFAULT_THRESHOLD,
        equivalent_identifier_cutoff: int = DEFAULT_CUTOFF,
    ) -> None:
        """Constructor.

        :param choose_edition: Should a new presentation edition be
           chosen/created, or should we assume the old one is fine?
        :param set_edition_metadata: Should we set new values for
           basic metadata such as title?
        :param classify: Should we reconsider which Genres under which
           a Work should be filed?
        :param choose_summary: Should we reconsider which of the
           available summaries is the best?
        :param calculate_quality: Should we recalculate the overall
           quality of the Work?
        :param choose_cover: Should we reconsider which of the
           available cover images is the best?
        :param update_search_index: Should we reindex this Work's
           entry in the search index?
        :param verbose: Should we print out information about the work we're
           doing?
        :param equivalent_identifier_levels: When determining which
           identifiers refer to this Work (used when gathering
           classifications, cover images, etc.), how many levels of
           equivalency should we go down? E.g. for one level of
           equivalency we will go from a proprietary vendor ID to the
           equivalent ISBN.
        :param equivalent_identifier_threshold: When determining which
           identifiers refer to this Work, what is the probability
           threshold for 'equivalency'? E.g. a value of 1 means that
           we will not count two identifiers as equivalent unless we
           are absolutely certain.
        :param equivalent_identifier_cutoff: When determining which
           identifiers refer to this work, how many Identifiers are
           enough? Gathering _all_ the identifiers that identify an
           extremely popular work can take an extraordinarily long time
           for very little payoff, so it's useful to have a cutoff.

           The cutoff is applied _per level_, so the total maximum
           number of equivalent identifiers is
           equivalent_identifier_cutoff * equivalent_identifier_levels.
        """
        self.choose_edition = choose_edition
        self.set_edition_metadata = set_edition_metadata
        self.classify = classify
        self.choose_summary = choose_summary
        self.calculate_quality = calculate_quality
        self.choose_cover = choose_cover

        # Similarly for update_search_index.
        self.update_search_index = update_search_index

        self.verbose = verbose

        self.equivalent_identifier_levels = equivalent_identifier_levels
        self.equivalent_identifier_threshold = equivalent_identifier_threshold
        self.equivalent_identifier_cutoff = equivalent_identifier_cutoff

    @classmethod
    def recalculate_everything(cls) -> Self:
        """A PresentationCalculationPolicy that always recalculates
        everything, even when it doesn't seem necessary.
        """
        return cls(
            update_search_index=True,
        )

    @classmethod
    def reset_cover(cls) -> Self:
        """A PresentationCalculationPolicy that only resets covers
        (including updating cached entries, if necessary) without
        impacting any other metadata.
        """
        return cls(
            choose_cover=True,
            choose_edition=False,
            set_edition_metadata=False,
            classify=False,
            choose_summary=False,
            calculate_quality=False,
        )
