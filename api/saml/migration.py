import argparse
import json
import logging
from json import JSONDecoder

from api.authenticator import BaseSAMLAuthenticationProvider, PatronData
from api.saml.configuration.model import SAMLConfiguration, SAMLConfigurationFactory
from api.saml.metadata.model import SAMLSubjectJSONDecoder, SAMLSubjectPatronIDExtractor
from api.saml.metadata.parser import SAMLMetadataParser
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model import Credential, Patron, Session
from core.model.configuration import (
    ConfigurationMetadata,
    ConfigurationStorage,
    ExternalIntegration,
)
from core.scripts import LibraryInputScript
from core.util.string_helpers import is_string


class SAMLMigrationManager(object):
    """Allows to change unique IDs of SAML patrons."""

    def __init__(self, saml_subject_json_decoder, patron_id_extractor):
        """Initialize a new instance of SAMLMigrationManager class.

        :param saml_subject_json_decoder: JSONDecoder instance used to extract a JSON-serialized SAML subject
            from Credential object stored in the database
        :type saml_subject_json_decoder: JSONDecoder

        :param patron_id_extractor: SAMLSubjectPatronIDExtractor instance used to extract a unique patron ID
            from the SAML subject
        :type patron_id_extractor: SAMLSubjectPatronIDExtractor
        """
        if not isinstance(saml_subject_json_decoder, JSONDecoder):
            raise ValueError(
                "'saml_subject_json_decoder' argument must be an instance of {0} class".format(
                    JSONDecoder.__class__
                )
            )
        if not isinstance(patron_id_extractor, SAMLSubjectPatronIDExtractor):
            raise ValueError(
                "'patron_id_extractor' argument must be an instance of {0} class".format(
                    SAMLSubjectPatronIDExtractor.__class__
                )
            )

        self._patron_id_extractor = patron_id_extractor
        self._saml_subject_json_decoder = saml_subject_json_decoder
        self._logger = logging.getLogger(__name__)

    def migrate(self, db, library_id):
        """Migrate all the SAML patrons in the database:
        - find existing SAML patrons
        - update their unique IDs according to the settings in `self._patron_id_extractor`.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param library_id: ID of the library patrons belong to
        :type library_id: int
        """
        self._logger.info(
            "Started migrating SAML patrons using the following settings: "
            "use NameID={0}, SAML attributes={1}, regular expression={2}".format(
                self._patron_id_extractor.use_name_id,
                ", ".join(self._patron_id_extractor.patron_id_attributes)
                if self._patron_id_extractor.patron_id_attributes
                else "[]",
                str(self._patron_id_extractor.patron_id_regular_expression),
            )
        )

        saml_credentials = (
            db.query(Credential)
            .join(Patron)
            .filter(Credential.type == BaseSAMLAuthenticationProvider.TOKEN_TYPE)
            .filter(Patron.library_id == library_id)
        )

        transaction = db.begin_nested()

        try:
            for saml_credential in saml_credentials:
                saml_subject = self._saml_subject_json_decoder.decode(
                    saml_credential.credential
                )
                patron_id = self._patron_id_extractor.extract(saml_subject)

                if not patron_id:
                    self._logger.warning(
                        "Could not find a unique patron ID in {0} for patron {1}".format(
                            saml_credential,
                            saml_credential.patron
                        )
                    )

                patron_data = PatronData(
                    permanent_id=patron_id,
                    authorization_identifier=patron_id,
                    external_type="A",
                    complete=True,
                )

                patron_data.apply(saml_credential.patron)

            transaction.commit()
            db.commit()
        except:
            self._logger.exception(
                "An unexpected exception occurred during migration of SAML patrons"
            )

            transaction.rollback()
            raise

        self._logger.info("Finished migrating SAML patrons")


class SAMLMigrationManagerFactory(object):
    """Used to create SAMLMigrationManager instances.

    This class simplifies creation of SAMLMigrationManager instances and allows to unit-test it.
    """

    def create(
        self,
        use_name_id=True,
        patron_id_attributes=None,
        patron_id_regular_expression=None,
    ):
        """Create a new instance of SAMLMigrationManager class.

        :param use_name_id: Boolean value indicating whether NameID should be searched for a unique patron ID
        :type use_name_id: bool

        :param patron_id_attributes: List of SAML attributes which should be searched for a unique patron ID
        :type patron_id_attributes: Optional[List[str]]

        :param patron_id_regular_expression: Regular expression used to extract a unique patron ID from SAML attributes
        :type patron_id_regular_expression: Optional[str]

        :return: SAMLMigrationManager object
        :rtype: SAMLMigrationManager
        """
        if not isinstance(use_name_id, bool):
            raise ValueError("'use_name_id' must be boolean")
        if patron_id_attributes is not None and not isinstance(patron_id_attributes, list):
            raise ValueError("'patron_id_attributes' must be a list")
        if patron_id_regular_expression is not None and not is_string(patron_id_regular_expression):
            raise ValueError("'patron_id_regular_expression' must be a string")

        saml_subject_json_decoder = SAMLSubjectJSONDecoder()
        patron_id_extractor = SAMLSubjectPatronIDExtractor(
            use_name_id, patron_id_attributes, patron_id_regular_expression
        )
        migration_manager = SAMLMigrationManager(
            saml_subject_json_decoder, patron_id_extractor
        )

        return migration_manager


class SAMLMigrationScript(LibraryInputScript):
    """Script running SAML patron ID migration logic."""

    def __init__(self, saml_migration_manager_factory, db=None, *args, **kwargs):
        """Initialize a new instance of SAMLMigrationScript class.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session
        """
        super(SAMLMigrationScript, self).__init__(db, *args, **kwargs)

        if not isinstance(saml_migration_manager_factory, SAMLMigrationManagerFactory):
            raise ValueError(
                "'saml_migration_manager_factory' argument must be an instance of {0} class".format(
                    SAMLMigrationManagerFactory.__name__
                )
            )

        self._saml_migration_manager_factory = saml_migration_manager_factory

    def _parse_command_line(self, library, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)

        if (
            not hasattr(parsed, "use_name_id")
            or not hasattr(parsed, "patron_id_attributes")
            or not hasattr(parsed, "patron_id_regex")
        ):
            db = Session.object_session(library)
            saml_authentication_provider_external_integration = (
                ExternalIntegration.lookup(
                    db,
                    "api.saml.provider",
                    ExternalIntegration.PATRON_AUTH_GOAL,
                    library,
                )
            )
            saml_authentication_provider = SAMLWebSSOAuthenticationProvider(
                library, saml_authentication_provider_external_integration
            )
            saml_configuration_storage = ConfigurationStorage(
                saml_authentication_provider
            )
            saml_configuration_factory = SAMLConfigurationFactory(SAMLMetadataParser())

            with saml_configuration_factory.create(
                saml_configuration_storage, self._db, SAMLConfiguration
            ) as configuration:
                if not hasattr(parsed, "use_name_id"):
                    parsed.use_name_id = ConfigurationMetadata.to_bool(
                        configuration.patron_id_use_name_id
                    )
                if not hasattr(parsed, "patron_id_attributes"):
                    parsed.patron_id_attributes = (
                        json.loads(configuration.patron_id_attributes)
                        if configuration.patron_id_attributes
                        else []
                    )
                if not hasattr(parsed, "patron_id_regex"):
                    parsed.patron_id_regex = configuration.patron_id_regular_expression

        # Make sure that `use_name_id` is always boolean.
        parsed.use_name_id = ConfigurationMetadata.to_bool(parsed.use_name_id)

        return parsed

    @classmethod
    def arg_parser(cls, db, multiple_libraries=True):
        parser = LibraryInputScript.arg_parser(db, multiple_libraries)
        parser.add_argument(
            "--use-name-id",
            dest="use_name_id",
            default=argparse.SUPPRESS,
            help="Boolean value indicating whether NameID should be searched for a unique patron ID. "
            "If the value is omitted, configuration setting Patron ID: SAML NameID will be used instead.",
            action="store",
            choices=[str(True), str(False)],
        )
        parser.add_argument(
            "--patron-id-attributes",
            dest="patron_id_attributes",
            default=argparse.SUPPRESS,
            help="List of SAML attributes which should be searched for a unique patron ID. "
            "If the value is omitted, configuration setting Patron ID: SAML Attributes will be used instead.",
            nargs="*",
        )
        parser.add_argument(
            "--patron-id-regex",
            dest="patron_id_regex",
            default=argparse.SUPPRESS,
            help="Regular expression used to extract a unique patron ID from SAML attributes. "
            "If the value is omitted, configuration setting Patron ID: Regular expression will be used instead.",
        )
        return parser

    def process_library(self, library, *args, **kwargs):
        parsed = self._parse_command_line(library, *args, **kwargs)
        migration_manager = self._saml_migration_manager_factory.create(
            parsed.use_name_id, parsed.patron_id_attributes, parsed.patron_id_regex
        )

        migration_manager.migrate(self._db, library.id)
