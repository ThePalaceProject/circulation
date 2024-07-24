from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism

FEEDBOOKS_AUDIO = "{}; protection={}".format(
    MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
    DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
)
