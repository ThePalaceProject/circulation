"""
Accessibility metadata models for Readium Web Publication Manifest.

https://github.com/readium/webpub-manifest/tree/master/contexts/default#accessibility-metadata
https://readium.org/webpub-manifest/schema/a11y.schema.json
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from enum import StrEnum, auto
from functools import cached_property
from typing import Any, cast

from pydantic import (
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
)

from palace.opds.base import BaseOpdsModel
from palace.opds.util import StrOrTuple, drop_if_falsy, obj_or_tuple_to_tuple

_logger = logging.getLogger(__name__)


def _coerce_enum_list(value: Any, enum_cls: type[StrEnum], field_name: str) -> Any:
    """Coerce a list of strings to enum members, logging and dropping unknown values.

    Performs a case-insensitive match against ``enum_cls`` values so that
    publications with miscased entries (e.g. ``"TaggedPDF"`` instead of
    ``"taggedPDF"``) still import. Unknown values are dropped with a warning
    so they can be reported upstream.
    """
    if not isinstance(value, list):
        return value
    lookup = {member.value.lower(): member for member in enum_cls}
    coerced: list[Any] = []
    for item in value:
        if isinstance(item, enum_cls):
            coerced.append(item)
            continue
        if isinstance(item, str):
            match = lookup.get(item.lower())
            if match is not None:
                if match.value != item:
                    _logger.warning(
                        "Coerced %s value %r to canonical %r",
                        field_name,
                        item,
                        match.value,
                    )
                coerced.append(match)
                continue
            _logger.warning("Dropping unknown %s value %r", field_name, item)
            continue
        coerced.append(item)
    return coerced


class AccessMode(StrEnum):
    """
    Human sensory perceptual system or cognitive faculty necessary to process
    or perceive the content of a publication.

    https://www.w3.org/2021/a11y-discov-vocab/latest/#accessMode-vocabulary
    https://readium.org/webpub-manifest/schema/a11y.schema.json
    """

    auditory = auto()
    chart_on_visual = "chartOnVisual"
    chem_on_visual = "chemOnVisual"
    color_dependent = "colorDependent"
    diagram_on_visual = "diagramOnVisual"
    math_on_visual = "mathOnVisual"
    music_on_visual = "musicOnVisual"
    tactile = auto()
    text_on_visual = "textOnVisual"
    textual = auto()
    visual = auto()


class AccessModeSufficient(StrEnum):
    """
    Single or combined access modes that are sufficient to understand the
    intellectual content of a publication.

    https://www.w3.org/2021/a11y-discov-vocab/latest/#accessModeSufficient-vocabulary
    https://readium.org/webpub-manifest/schema/a11y.schema.json
    """

    auditory = auto()
    tactile = auto()
    textual = auto()
    visual = auto()


class AccessibilityFeature(StrEnum):
    """
    Accessibility features of a publication.

    https://www.w3.org/2021/a11y-discov-vocab/latest/#accessibilityFeature-vocabulary
    https://readium.org/webpub-manifest/schema/a11y.schema.json
    """

    # Navigation and structure
    annotations = auto()
    ARIA = "ARIA"
    bookmarks = auto()
    book_index = "index"
    page_break_markers = "pageBreakMarkers"
    print_page_numbers = "printPageNumbers"
    page_navigation = "pageNavigation"
    reading_order = "readingOrder"
    structural_navigation = "structuralNavigation"
    table_of_contents = "tableOfContents"
    tagged_pdf = "taggedPDF"

    # Alternative representations
    alternative_text = "alternativeText"
    audio_description = "audioDescription"
    closed_captions = "closedCaptions"
    captions = auto()
    described_math = "describedMath"
    long_description = "longDescription"
    open_captions = "openCaptions"
    sign_language = "signLanguage"
    transcript = auto()

    # Adaptability
    display_transformability = "displayTransformability"
    synchronized_audio_text = "synchronizedAudioText"
    timing_control = "timingControl"
    unlocked = auto()

    # Math and chemistry markup
    chem_ml = "ChemML"
    latex = auto()
    latex_chemistry = "latex-chemistry"
    math_ml = "MathML"
    math_ml_chemistry = "MathML-chemistry"

    # Text-to-speech
    tts_markup = "ttsMarkup"

    # Audio adjustments
    high_contrast_audio = "highContrastAudio"

    # Visual adjustments
    high_contrast_display = "highContrastDisplay"
    large_print = "largePrint"

    # Tactile
    braille = auto()
    tactile_graphic = "tactileGraphic"
    tactile_object = "tactileObject"

    # CJK and ruby
    full_ruby_annotations = "fullRubyAnnotations"
    horizontal_writing = "horizontalWriting"
    ruby_annotations = "rubyAnnotations"
    vertical_writing = "verticalWriting"
    with_additional_word_segmentation = "withAdditionalWordSegmentation"
    without_additional_word_segmentation = "withoutAdditionalWordSegmentation"

    # Metadata values
    none = auto()
    unknown = auto()


class AccessibilityHazard(StrEnum):
    """
    Potential hazards of a publication.

    https://www.w3.org/2021/a11y-discov-vocab/latest/#accessibilityHazard-vocabulary
    https://readium.org/webpub-manifest/schema/a11y.schema.json
    """

    flashing = auto()
    motion_simulation = "motionSimulation"
    sound = auto()
    none = auto()
    no_flashing_hazard = "noFlashingHazard"
    no_motion_simulation_hazard = "noMotionSimulationHazard"
    no_sound_hazard = "noSoundHazard"
    unknown = auto()
    unknown_flashing_hazard = "unknownFlashingHazard"
    unknown_motion_simulation_hazard = "unknownMotionSimulationHazard"
    unknown_sound_hazard = "unknownSoundHazard"


class Exemption(StrEnum):
    """
    Jurisdictional exemptions applicable to the publication.

    https://readium.org/webpub-manifest/schema/a11y.schema.json
    """

    eaa_disproportionate_burden = "eaa-disproportionate-burden"
    eaa_fundamental_alteration = "eaa-fundamental-alteration"
    eaa_microenterprise = "eaa-microenterprise"


class Certification(BaseOpdsModel):
    """
    Accessibility certification information.

    https://github.com/readium/webpub-manifest/tree/master/contexts/default#certification
    https://readium.org/webpub-manifest/schema/a11y.schema.json
    """

    certified_by: str | None = Field(None, alias="certifiedBy")
    credential: str | None = None
    report: str | None = None


class Accessibility(BaseOpdsModel):
    """
    Accessibility metadata for a publication.

    https://github.com/readium/webpub-manifest/tree/master/contexts/default#accessibility-metadata
    https://readium.org/webpub-manifest/schema/a11y.schema.json
    """

    conforms_to: StrOrTuple[str] | None = Field(None, alias="conformsTo")

    @cached_property
    def conformance_profiles(self) -> Sequence[str]:
        return obj_or_tuple_to_tuple(self.conforms_to)

    exemption: Exemption | None = None
    access_mode: list[AccessMode] = Field(default_factory=list, alias="accessMode")
    access_mode_sufficient: list[StrOrTuple[AccessModeSufficient]] = Field(
        default_factory=list, alias="accessModeSufficient"
    )

    @cached_property
    def sufficient_access_modes(self) -> Sequence[Sequence[AccessModeSufficient]]:
        """Normalize each item in access_mode_sufficient to a tuple."""
        return tuple(
            obj_or_tuple_to_tuple(item) for item in self.access_mode_sufficient
        )

    feature: list[AccessibilityFeature] = Field(default_factory=list)
    hazard: list[AccessibilityHazard] = Field(default_factory=list)
    certification: Certification | None = None
    summary: str | None = None

    @field_validator("feature", mode="before")
    @classmethod
    def _coerce_features(cls, value: Any) -> Any:
        return _coerce_enum_list(value, AccessibilityFeature, "feature")

    @field_validator("hazard", mode="before")
    @classmethod
    def _coerce_hazards(cls, value: Any) -> Any:
        return _coerce_enum_list(value, AccessibilityHazard, "hazard")

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))
        drop_if_falsy(self, "conforms_to", data)
        drop_if_falsy(self, "exemption", data)
        drop_if_falsy(self, "access_mode", data)
        drop_if_falsy(self, "access_mode_sufficient", data)
        drop_if_falsy(self, "feature", data)
        drop_if_falsy(self, "hazard", data)
        drop_if_falsy(self, "certification", data)
        drop_if_falsy(self, "summary", data)
        return data
