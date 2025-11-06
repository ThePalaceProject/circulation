from logging import Logger

from celery import shared_task
from sqlalchemy.orm import Session

from palace.manager.celery.task import Task
from palace.manager.integration.patron_auth.saml.metadata.federations.loader import (
    SAMLFederatedIdentityProviderLoader,
    SAMLMetadataLoader,
)
from palace.manager.integration.patron_auth.saml.metadata.federations.validator import (
    SAMLFederatedMetadataExpirationValidator,
    SAMLFederatedMetadataValidatorChain,
    SAMLMetadataSignatureValidator,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLMetadataParser,
)
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.saml import SAMLFederation
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerAdapterType


def _update_saml_federation_idps_metadata(
    saml_federation: SAMLFederation,
    loader: SAMLFederatedIdentityProviderLoader,
    session: Session,
    log: Logger | LoggerAdapterType,
) -> None:
    """Update IdPs' metadata belonging to the specified SAML federation."""
    log.info(f"Started processing {saml_federation}")

    for existing_identity_provider in saml_federation.identity_providers:
        session.delete(existing_identity_provider)

    new_identity_providers = loader.load(saml_federation)

    for new_identity_provider in new_identity_providers:
        session.add(new_identity_provider)
        new_identity_provider

    saml_federation.last_updated_at = utc_now()

    log.info(f"Finished processing {saml_federation}")


@shared_task(queue=QueueNames.default, bind=True)
def update_saml_federation_idps_metadata(task: Task) -> None:
    """
    For each SAML Federation in the CM, update the federations IdPs metadata
    Please note that the monitor looks for federations in the `samlfederations` table.
    Currently, there is no way to configure SAML federations in the admin interface.
    """

    saml_federated_idp_loader = _create_saml_federated_identity_provider_loader()

    # Note that we need to use a transaction here, since we are going
    # to update the database.
    with task.transaction() as session:
        saml_federations = session.query(SAMLFederation).all()

        task.log.info(f"Found {len(saml_federations)} SAML federations")

        for outdated_saml_federation in saml_federations:
            _update_saml_federation_idps_metadata(
                outdated_saml_federation,
                saml_federated_idp_loader,
                session,
                task.log,
            )

    task.log.info("Finished updating the SAML metadata")


def _create_saml_federated_identity_provider_loader() -> (
    SAMLFederatedIdentityProviderLoader
):
    saml_metadata_loader = SAMLMetadataLoader()
    saml_metadata_validator = SAMLFederatedMetadataValidatorChain(
        [SAMLFederatedMetadataExpirationValidator(), SAMLMetadataSignatureValidator()]
    )
    saml_metadata_parser = SAMLMetadataParser(skip_incorrect_providers=True)
    return SAMLFederatedIdentityProviderLoader(
        saml_metadata_loader, saml_metadata_validator, saml_metadata_parser
    )
