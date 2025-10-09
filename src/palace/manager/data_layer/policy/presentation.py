from __future__ import annotations

from typing import Self

from palace.manager.data_layer.base.frozen import BaseFrozenData


class PresentationCalculationPolicy(BaseFrozenData):
    """Which parts of the Work or Edition's presentation
    are we actually looking to update?
    """

    choose_edition: bool = True
    """
    Should a new presentation edition be
    chosen/created, or should we assume the old one is fine?
    """

    set_edition_metadata: bool = True
    """
    Should we set new values for basic metadata such as title?
    """

    classify: bool = True
    """
    Should we reconsider which Genres under which a Work should be filed?
    """

    choose_summary: bool = True
    """
    Should we reconsider which of the available summaries is the best?
    """

    calculate_quality: bool = True
    """
    Should we recalculate the overall quality of the Work?
    """

    choose_cover: bool = True
    """
    Should we reconsider which of the available cover images is the best?
    """

    update_search_index: bool = False
    """
    Should we reindex this Work's entry in the search index?
    """

    verbose: bool = True
    """
    Should we print out information about the work we're doing?
    """

    equivalent_identifier_levels: int = 3
    """
    When determining which identifiers refer to this Work (used when gathering
    classifications, cover images, etc.), how many levels of
    equivalency should we go down? E.g. for one level of
    equivalency we will go from a proprietary vendor ID to the
    equivalent ISBN.
    """

    equivalent_identifier_threshold: float = 0.5
    """
    When determining which
    identifiers refer to this Work, what is the probability
    threshold for 'equivalency'? E.g. a value of 1 means that
    we will not count two identifiers as equivalent unless we
    are absolutely certain.
    """

    equivalent_identifier_cutoff: int = 1000
    """
    When determining which
    identifiers refer to this work, how many Identifiers are
    enough? Gathering _all_ the identifiers that identify an
    extremely popular work can take an extraordinarily long time
    for very little payoff, so it's useful to have a cutoff.

    The cutoff is applied _per level_, so the total maximum
    number of equivalent identifiers is
    equivalent_identifier_cutoff * equivalent_identifier_levels.
    """

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

    @classmethod
    def recalculate_presentation_edition(cls) -> Self:
        return cls(
            choose_edition=True,
            set_edition_metadata=True,
            verbose=True,
            # These are the expensive ones, and they're covered by
            # recalculate_everything
            classify=False,
            choose_summary=False,
            calculate_quality=False,
            # It would be better if there were a separate class for this
            # operation (COVER_OPERATION)
            choose_cover=True,
            # This will flag the Work as needing a search index update
            update_search_index=True,
        )

    @classmethod
    def recalculate_classification(cls) -> Self:
        return cls(
            choose_edition=False,
            set_edition_metadata=False,
            classify=True,
            choose_summary=False,
            calculate_quality=False,
            choose_cover=False,
            update_search_index=False,
        )
