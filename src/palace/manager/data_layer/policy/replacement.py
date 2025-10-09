from __future__ import annotations

from typing import Self

from pydantic import Field
from sqlalchemy.orm import Session
from typing_extensions import TypedDict, Unpack

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)


class _ReplacementPolicyKwargs(TypedDict, total=False):
    even_if_not_apparently_updated: bool
    link_content: bool
    presentation_calculation_policy: PresentationCalculationPolicy


class ReplacementPolicy(BaseFrozenData):
    """How serious should we be about overwriting old metadata with
    this new metadata?
    """

    identifiers: bool = False
    subjects: bool = False
    contributions: bool = False
    links: bool = False
    formats: bool = False
    rights: bool = False
    link_content: bool = False
    even_if_not_apparently_updated: bool = False
    presentation_calculation_policy: PresentationCalculationPolicy = Field(
        default_factory=PresentationCalculationPolicy
    )

    @classmethod
    def from_license_source(
        cls,
        _db: Session,
        **kwargs: Unpack[_ReplacementPolicyKwargs],
    ) -> Self:
        """When gathering data from the license source, overwrite all old data
        from this source with new data from the same source. Also
        overwrite an old rights status with an updated status and update
        the list of available formats. Log availability changes to the
        configured analytics services.
        """
        return cls(
            identifiers=True,
            subjects=True,
            contributions=True,
            links=True,
            rights=True,
            formats=True,
            **kwargs,
        )

    @classmethod
    def from_metadata_source(cls, **kwargs: Unpack[_ReplacementPolicyKwargs]) -> Self:
        """When gathering data from a metadata source, overwrite all old data
        from this source, but do not overwrite the rights status or
        the available formats. License sources are the authority on rights
        and formats, and metadata sources have no say in the matter.
        """
        return cls(
            identifiers=True,
            subjects=True,
            contributions=True,
            links=True,
            rights=False,
            formats=False,
            **kwargs,
        )

    @classmethod
    def append_only(cls, **kwargs: Unpack[_ReplacementPolicyKwargs]) -> Self:
        """Don't overwrite any information, just append it.

        This should probably never be used.
        """
        return cls(
            identifiers=False,
            subjects=False,
            contributions=False,
            links=False,
            rights=False,
            formats=False,
            **kwargs,
        )
