#!/usr/bin/env python
"""Adds InCommon SAML federation metadata to `samlfederations` table."""

from contextlib2 import closing

from palace.api.saml.metadata.federations import incommon
from palace.api.saml.metadata.federations.model import SAMLFederation
from palace.core.model import production_session

with closing(production_session()) as db:
    incommon_federation = (
        db.query(SAMLFederation)
        .filter(SAMLFederation.type == incommon.FEDERATION_TYPE)
        .one_or_none()
    )

    if not incommon_federation:
        incommon_federation = SAMLFederation(
            incommon.FEDERATION_TYPE,
            incommon.IDP_METADATA_SERVICE_URL,
            incommon.CERTIFICATE,
        )

        db.add(incommon_federation)
        db.commit()
