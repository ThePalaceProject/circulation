import uuid

from core.model import Collection, ExternalIntegration, Library, get_one_or_create


def make_default_library(_db):
    """DEPRECATED DO NOT USE
    Ensure that the default library exists in the given database.

    This can be called by code intended for use in testing but not actually
    within a DatabaseTest subclass.
    """
    library, ignore = get_one_or_create(
        _db,
        Library,
        create_method_kwargs=dict(
            uuid=str(uuid.uuid4()),
            name="default",
        ),
        short_name="default",
    )
    collection, ignore = get_one_or_create(_db, Collection, name="Default Collection")
    integration = collection.create_external_integration(
        ExternalIntegration.OPDS_IMPORT
    )
    integration.goal = ExternalIntegration.LICENSE_GOAL

    if collection not in library.collections:
        library.collections.append(collection)

    config = collection.create_integration_configuration(
        ExternalIntegration.OPDS_IMPORT
    )
    config.for_library(library.id, create=True)
    return library
