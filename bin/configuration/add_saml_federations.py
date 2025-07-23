#!/usr/bin/env python
"""Adds SAML federation metadata to `samlfederations` table."""

from contextlib import closing

from palace.manager.integration.patron_auth.saml.metadata.federations import incommon
from palace.manager.sqlalchemy.model.saml import SAMLFederation
from palace.manager.sqlalchemy.session import production_session

with closing(production_session("add_saml_federations")) as db:
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
