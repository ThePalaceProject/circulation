from __future__ import annotations

from enum import StrEnum

from bidict import frozenbidict
from frozendict import frozendict

from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    DeliveryMechanismTuple,
)
from palace.manager.sqlalchemy.model.resource import Representation


class BoundlessFormat(StrEnum):
    axis_now = "AxisNow"
    # Legacy format, handled the same way as AxisNow
    blio = "Blio"
    epub = "ePub"
    pdf = "PDF"
    acoustik = "Acoustik"


class ServerNickname(StrEnum):
    production = "Production"
    qa = "QA"


API_BASE_URLS = frozendict(
    {
        ServerNickname.production: "https://axis360api.baker-taylor.com/Services/VendorAPI/",
        ServerNickname.qa: "https://axis360apiqa.baker-taylor.com/Services/VendorAPI/",
    }
)

LICENSE_SERVER_BASE_URLS = frozendict(
    {
        ServerNickname.production: "https://frontdoor.axisnow.com/",
        ServerNickname.qa: "https://qa-frontdoor.axisnow.com/",
    }
)


DELIVERY_MECHANISM_TO_INTERNAL_FORMAT: frozenbidict[DeliveryMechanismTuple, str] = (
    frozenbidict(
        {
            DeliveryMechanismTuple(
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
            ): BoundlessFormat.epub,
            DeliveryMechanismTuple(
                Representation.PDF_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
            ): BoundlessFormat.pdf,
            DeliveryMechanismTuple(
                None,
                DeliveryMechanism.FINDAWAY_DRM,
            ): BoundlessFormat.acoustik,
            DeliveryMechanismTuple(
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM,
            ): BoundlessFormat.axis_now,
        }
    )
)

INTERNAL_FORMAT_TO_DELIVERY_MECHANISM = DELIVERY_MECHANISM_TO_INTERNAL_FORMAT.inverse

BAKER_TAYLOR_KDRM_PARAMS = ("modulus", "exponent", "device_id")

AXIS_360_PROTOCOL = "Axis 360"
