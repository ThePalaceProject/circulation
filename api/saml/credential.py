import datetime
import json
import logging
from copy import deepcopy

import sqlalchemy

from api.saml.metadata.model import (
    SAMLNameIDFormat,
    SAMLSubject,
    SAMLSubjectJSONDecoder,
    SAMLSubjectJSONEncoder,
)
from core.model import Credential, DataSource, Patron, get_one_or_create


class SAMLCredentialManager:
    """Manages SAML tokens.

    By SAML tokens we may mean two different things:
        - an encoded string containing a serialized SAML Subject object uniquely describing
            a patron authenticated using SAML protocol;
        - a Credential object containing the  SAML token.
    """

    TOKEN_TYPE = "SAML 2.0 token"
    TOKEN_DATA_SOURCE_NAME = "SAML 2.0"

    def __init__(self):
        """Initialize a new instance of SAMLCredentialManager class."""
        self._logger: logging.Logger = logging.getLogger(__name__)

    def _get_token_data_source(self, db: sqlalchemy.orm.session.Session) -> DataSource:
        """Return a data source used to store SAML credentials.

        :param db: Database session
        :return: Data source used to store SAML credentials
        """
        # FIXME: This code will probably not work in a situation where a library has multiple SAML
        #  authentication mechanisms for its patrons.
        #  It'll look up a Credential from this data source but it won't be able to tell which IdP it came from.
        datasource, _ = get_one_or_create(
            db, DataSource, name=self.TOKEN_DATA_SOURCE_NAME
        )

        return datasource

    @staticmethod
    def _create_saml_token_value(subject: SAMLSubject) -> str:
        """Create a SAML token by serializing the SAML subject.

        :param subject: SAML subject
        :return: SAML token
        """
        subject = deepcopy(subject)

        # We should not save a transient Name ID because it changes each time
        if (
            subject.name_id
            and subject.name_id.name_format == SAMLNameIDFormat.TRANSIENT.value
        ):
            subject.name_id = None

        token_value = json.dumps(subject, cls=SAMLSubjectJSONEncoder)

        return token_value

    def extract_saml_token(self, credential: Credential) -> SAMLSubject:
        """Extract a SAML subject from SAML token.

        :param credential: Credential object containing a SAML token
        :return: SAML subject
        """
        self._logger.debug(f"Started deserializing SAML token {credential}")

        credential_value = credential.credential if credential.credential else "{}"

        subject = json.loads(credential_value, cls=SAMLSubjectJSONDecoder)

        self._logger.debug(f"Finished deserializing SAML token {credential}: {subject}")

        return subject

    def create_saml_token(
        self,
        db: sqlalchemy.orm.session.Session,
        patron: Patron,
        subject: SAMLSubject,
        cm_session_lifetime: int | None = None,
    ) -> Credential:
        """Create a Credential object that ties the given patron to the given provider token.

        :param db: Database session
        :param patron: Patron object
        :param subject: SAML subject
        :param cm_session_lifetime: (Optional) Circulation Manager's session lifetime expressed in days
        :return: Credential object
        """
        session_lifetime = subject.valid_till

        if cm_session_lifetime:
            session_lifetime = datetime.timedelta(days=int(cm_session_lifetime))

        token = self._create_saml_token_value(subject)
        data_source = self._get_token_data_source(db)

        saml_token, _ = Credential.temporary_token_create(
            db, data_source, self.TOKEN_TYPE, patron, session_lifetime, token
        )

        return saml_token

    def lookup_saml_token_by_patron(
        self, db: sqlalchemy.orm.session.Session, patron: Patron
    ) -> Credential | None:
        """Look up for a SAML token.

        :param db: Database session
        :param patron: Patron object
        :return: SAML subject (if any)
        """
        self._logger.debug("Started looking up for a SAML token")

        credential = Credential.lookup_by_patron(
            db,
            self.TOKEN_DATA_SOURCE_NAME,
            self.TOKEN_TYPE,
            patron,
            allow_persistent_token=False,
            auto_create_datasource=True,
        )

        self._logger.debug(f"Finished looking up for a SAML token: {credential}")

        return credential

    def lookup_saml_token_by_value(
        self, db: sqlalchemy.orm.session.Session, token: dict
    ) -> Credential | None:
        """Look up for a SAML token.

        :param db: Database session
        :param token: SAML token
        :return: SAML subject (if any)
        """
        self._logger.debug("Started looking up for a SAML token")

        credential = Credential.lookup_by_token(
            db, self._get_token_data_source(db), self.TOKEN_TYPE, token
        )

        self._logger.debug(f"Finished looking up for a SAML token: {credential}")

        return credential
