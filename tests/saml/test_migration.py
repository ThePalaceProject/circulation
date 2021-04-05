import json
import sys

from api.saml.configuration.model import SAMLConfiguration, SAMLConfigurationFactory
from api.saml.metadata.model import (
    SAMLAttribute,
    SAMLAttributeStatement,
    SAMLAttributeType,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLSubject,
)
from api.saml.metadata.parser import SAMLMetadataParser
from api.saml.migration import SAMLMigrationManagerFactory, SAMLMigrationScript
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model.configuration import ConfigurationStorage
from mock import MagicMock
from parameterized import parameterized
from tests.saml import fixtures
from tests.saml.database_test import DatabaseTest


class TestSAMLMigrationManager(DatabaseTest):
    def setup_method(self, _db=None, set_up_circulation_manager=True):
        super(TestSAMLMigrationManager, self).setup_method()

        metadata_parser = SAMLMetadataParser()

        self._configuration_storage = ConfigurationStorage(
            self._authentication_provider
        )
        self._configuration_factory = SAMLConfigurationFactory(metadata_parser)

    def test_migrate_migrates_patrons_with_old_ids(self):
        """Ensure that migration logic works correctly. This test case emulates the following scenario:
        1. The administrator set up the SAML authentication provider and left "Patron ID" configuration settings
        untouched making Circulation Manager use the default ("old") patron ID extraction algorithm.
        2. Patrons authenticated using SAML.
        3. Circulation Manager used the default ("old") patron ID extraction algorithm,
        extracted "old" ID from the SAML NameID and created Patron objects.
        4. Patrons checked out some books and had some books on hold that were associated with their Patron objects.
        5. The administrator updated the "Patron ID" configuration settings:
        5.1. Overrode the list of SAML attributes and added eduPersonPrincipalName attribute.
        5.2. Set up a custom regular expression to extract a patron ID from eduPersonPrincipalName.
        6. The administrator ran saml_migrate script to update IDs of the SAML patrons and set it to the value
        containing in the eduPersonPrincipalName attribute.
        7. SAML session expired.
        8. The same patrons reauthenticated using SAML.
        9. Circulation Manager using the new patron ID extraction settings, extracted patron IDs from
        eduPersonPrincipalName SAML attribute's and found the existing patrons in the database,
        the same patrons that were created in 3 and updated in 6 and who have the holds and loans.
        """
        # Arrange
        patron_1_old_id = "patron_1_old_id"
        patron_1_new_id = "patron_1_new_id"

        patron_2_old_id = "patron_2_old_id"
        patron_2_new_id = "patron_2_new_id"

        patron_1_subject = SAMLSubject(
            SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", patron_1_old_id),
            SAMLAttributeStatement(
                [
                    SAMLAttribute(
                        name=SAMLAttributeType.eduPersonPrincipalName.name,
                        values=["{0}@university.org".format(patron_1_new_id)],
                    )
                ]
            ),
        )
        patron_2_subject = SAMLSubject(
            SAMLNameID(SAMLNameIDFormat.UNSPECIFIED.value, "", "", patron_2_old_id),
            SAMLAttributeStatement(
                [
                    SAMLAttribute(
                        name=SAMLAttributeType.eduPersonPrincipalName.name,
                        values=["{0}@university.org".format(patron_2_new_id)],
                    )
                ]
            ),
        )

        edition, licensepool = self._edition(
            with_license_pool=True,
            with_open_access_download=False,
        )

        # Act, assert

        # 1. The administrator set up the SAML authentication provider
        # using the default "Patron ID" configuration settings.
        with self._configuration_factory.create(
            self._configuration_storage, self._db, SAMLConfiguration
        ) as configuration:
            configuration.patron_id_use_name_id = str(True)

        provider = SAMLWebSSOAuthenticationProvider(
            self._default_library, self._integration
        )

        # 2. Patrons authenticated using SAML.
        # 3. Circulation Manager extracted the "old" patron ID.
        _, patron_1_old_object, patron_1_old_data = provider.saml_callback(
            self._db, patron_1_subject
        )

        assert patron_1_old_id == patron_1_old_object.authorization_identifier
        assert patron_1_old_id == patron_1_old_object.external_identifier

        _, patron_2_old_object, patron_2_old_data = provider.saml_callback(
            self._db, patron_2_subject
        )

        assert patron_2_old_id == patron_2_old_object.authorization_identifier
        assert patron_2_old_id == patron_2_old_object.external_identifier

        # 4. The patrons checked out some books and had some books on hold.
        patron_1_hold, _ = licensepool.loan_to(patron_1_old_object)
        patron_1_hold, _ = licensepool.on_hold_to(patron_1_old_object)

        patron_2_hold, _ = licensepool.loan_to(patron_2_old_object)
        patron_2_hold, _ = licensepool.on_hold_to(patron_2_old_object)

        # 5. The administrator updated the "Patron ID" configuration settings.
        with self._configuration_factory.create(
            self._configuration_storage, self._db, SAMLConfiguration
        ) as configuration:
            configuration.patron_id_attributes = json.dumps(
                [SAMLAttributeType.eduPersonPrincipalName.name]
            )
            configuration.patron_id_regular_expression = (
                fixtures.PATRON_ID_REGULAR_EXPRESSION_ORG
            )

        # 6. The administrator ran saml_migrate script.
        saml_migration_manager_factory = SAMLMigrationManagerFactory()
        migration_script = SAMLMigrationScript(saml_migration_manager_factory, self._db)

        sys.argv = ["saml_migrate", self._default_library.short_name]
        migration_script.run()

        # 7. SAML session expired.

        provider = SAMLWebSSOAuthenticationProvider(
            self._default_library, self._integration
        )

        # 8. The same patrons reauthenticated using SAML.
        # 9. Circulation Manager using the new patron ID extraction settings, extracted patron IDs from
        # eduPersonPrincipalName SAML attribute and found the existing patrons in the database,
        # the same patrons that were created in 3 and who have the holds and loans.
        _, patron_1_new_object, patron_1_new_data = provider.saml_callback(
            self._db, patron_1_subject
        )
        _, patron_2_new_object, patron_2_new_data = provider.saml_callback(
            self._db, patron_2_subject
        )

        # Assert
        # Ensure that all the holds and loans were successfully transferred.
        assert patron_1_new_object == patron_1_hold.patron
        assert patron_1_new_object == patron_1_hold.patron

        assert patron_2_new_object == patron_2_hold.patron
        assert patron_2_new_object == patron_2_hold.patron

        # Ensure that patron has the "new" ID (value of the eduPersonPrincipalName SAML attribute).
        assert patron_1_new_id == patron_1_new_object.authorization_identifier
        assert patron_1_new_id == patron_1_new_object.external_identifier

        assert patron_2_new_id == patron_2_new_object.authorization_identifier
        assert patron_2_new_id == patron_2_new_object.external_identifier


class TestSAMLMigrationScript(DatabaseTest):
    def setup_method(self, _db=None, set_up_circulation_manager=True):
        super(TestSAMLMigrationScript, self).setup_method()

        metadata_parser = SAMLMetadataParser()

        self._configuration_storage = ConfigurationStorage(
            self._authentication_provider
        )
        self._configuration_factory = SAMLConfigurationFactory(metadata_parser)

    @parameterized.expand(
        [
            (
                "default_database_settings",
                None,
                None,
                None,
                SAMLConfiguration.patron_id_use_name_id.default,
                json.dumps(SAMLConfiguration.patron_id_attributes.default),
                SAMLConfiguration.patron_id_regular_expression.default,
                True,
                SAMLConfiguration.patron_id_attributes.default,
                SAMLConfiguration.patron_id_regular_expression.default,
            ),
            (
                "use_name_id_passed_as_command_line_argument",
                False,
                None,
                None,
                SAMLConfiguration.patron_id_use_name_id.default,
                json.dumps(SAMLConfiguration.patron_id_attributes.default),
                SAMLConfiguration.patron_id_regular_expression.default,
                False,
                SAMLConfiguration.patron_id_attributes.default,
                SAMLConfiguration.patron_id_regular_expression.default,
            ),
            (
                "use_name_id_and_attributes_passed_as_command_line_argument",
                False,
                [
                    SAMLAttributeType.givenName.name,
                    SAMLAttributeType.surname.name,
                    SAMLAttributeType.displayName.name,
                ],
                None,
                SAMLConfiguration.patron_id_use_name_id.default,
                json.dumps(SAMLConfiguration.patron_id_attributes.default),
                SAMLConfiguration.patron_id_regular_expression.default,
                False,
                [
                    SAMLAttributeType.givenName.name,
                    SAMLAttributeType.surname.name,
                    SAMLAttributeType.displayName.name,
                ],
                SAMLConfiguration.patron_id_regular_expression.default,
            ),
            (
                "use_name_id_attributes_and_regex_passed_as_command_line_argument",
                False,
                [
                    SAMLAttributeType.givenName.name,
                    SAMLAttributeType.surname.name,
                    SAMLAttributeType.displayName.name,
                ],
                fixtures.PATRON_ID_REGULAR_EXPRESSION_ORG,
                SAMLConfiguration.patron_id_use_name_id.default,
                json.dumps(SAMLConfiguration.patron_id_attributes.default),
                SAMLConfiguration.patron_id_regular_expression.default,
                False,
                [
                    SAMLAttributeType.givenName.name,
                    SAMLAttributeType.surname.name,
                    SAMLAttributeType.displayName.name,
                ],
                fixtures.PATRON_ID_REGULAR_EXPRESSION_ORG,
            ),
        ]
    )
    def test(
        self,
        _,
        command_line_use_name_id,
        command_line_patron_id_attributes,
        command_line_patron_id_regex,
        database_use_name_id,
        database_patron_id_attributes,
        database_patron_id_regex,
        expected_use_name_id,
        expected_patron_id_attributes,
        expected_patron_id_regex,
    ):
        """Ensure that SAMLMigrationScript correctly parses command-line arguments and
        substitutes them with database values if they're missing.

        This test tries different combinations of explicit command-line arguments and database configuration settings
        and makes sure that the SAML patron migration process is always launched using correct parameters.

        :param command_line_use_name_id: Command-line argument containing
            a boolean value indicating whether NameID should be searched for a unique patron ID.
            NOTE: It should be boolean, it will be converted into string in the test body.
        :type command_line_use_name_id: bool

        :param command_line_patron_id_attributes: Command-line argument containing
            a list of SAML attributes which should be searched for a unique patron ID
        :type command_line_patron_id_attributes: List[str]

        :param command_line_patron_id_regex: Command-line argument containing
            regular expression used to extract a unique patron ID from SAML attributes
        :type command_line_patron_id_regex: str

        :param database_use_name_id: Value of Patron ID: SAML NameID configuration setting
        :type database_use_name_id: bool

        :param database_patron_id_attributes: Value of Patron ID: SAML Attributes configuration setting
        :type database_patron_id_attributes: List[str]

        :param database_patron_id_regex: Value of Patron ID: Regular expression configuration setting
        :type database_patron_id_regex: str

        :param expected_use_name_id: Expected value of Use SAML Name ID setting
            that will be used by SAMLSubjectPatronIDExtractor
        :type expected_use_name_id: bool

        :param expected_patron_id_attributes: Expected value of SAML Patron ID Attributes setting
            that will be used by SAMLSubjectPatronIDExtractor
        :type expected_patron_id_attributes: List[str]

        :param expected_patron_id_regex: Expected value of SAML Patron ID Regular Expression setting
            that will be used by SAMLSubjectPatronIDExtractor
        :type expected_patron_id_regex: str
        """
        # Arrange
        command_line_arguments = ["saml_migrate"]

        if command_line_use_name_id is not None:
            command_line_arguments.extend(
                ["--use-name-id", str(command_line_use_name_id)]
            )
        if command_line_patron_id_attributes is not None:
            command_line_arguments.append("--patron-id-attributes")
            command_line_arguments.extend(command_line_patron_id_attributes)
        if command_line_patron_id_regex is not None:
            command_line_arguments.extend(
                ["--patron-id-regex", command_line_patron_id_regex]
            )

        if (
            database_use_name_id
            or database_patron_id_attributes
            or database_patron_id_regex
        ):
            with self._configuration_factory.create(
                self._configuration_storage, self._db, SAMLConfiguration
            ) as configuration:
                if database_use_name_id:
                    configuration.patron_id_use_name_id = database_use_name_id
                if database_patron_id_attributes:
                    configuration.patron_id_attributes = database_patron_id_attributes
                if database_patron_id_regex:
                    configuration.patron_id_regular_expression = (
                        database_patron_id_regex
                    )

        sys.argv = command_line_arguments
        saml_migration_manager_factory = SAMLMigrationManagerFactory()
        saml_migration_manager_factory.create = MagicMock(
            side_effect=saml_migration_manager_factory.create
        )

        migration_script = SAMLMigrationScript(saml_migration_manager_factory, self._db)

        # Act
        migration_script.run()

        # Assert
        saml_migration_manager_factory.create.assert_called_once_with(
            expected_use_name_id,
            expected_patron_id_attributes,
            expected_patron_id_regex,
        )
