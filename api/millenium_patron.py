import datetime
import re
from enum import Enum
from typing import List, Optional, Type, Union
from urllib import parse

import dateutil
from flask_babel import lazy_gettext as _
from lxml import etree
from money import Money
from pydantic import HttpUrl, validator

from core.analytics import Analytics
from core.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.model import Patron
from core.util import MoneyUtility
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.http import HTTP
from core.util.xmlparser import XMLParser

from .authentication.base import PatronData
from .authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)


class NeighborhoodMode(Enum):
    DISABLED = "disabled"
    HOME_BRANCH = "home_branch"
    POSTAL_CODE = "postal_code"


class AuthenticationMode(Enum):
    PIN = "pin"
    FAMILY_NAME = "family_name"


class MilleniumPatronSettings(BasicAuthProviderSettings):
    @validator("neighborhood_mode", pre=True)
    def validate_neighborhood_mode(cls, v):
        # TODO: We should fix this in the admin ui interface.
        #  For the neighborhood_mode setting, the admin UI isn't sending the
        #  default value, unless the user has changed it. Which causes us to
        #  fail validation. So if no option is selected, we use the default.
        if v is None:
            return NeighborhoodMode.DISABLED
        else:
            return v

    url: HttpUrl = FormField(
        ...,
        form=ConfigurationFormItem(
            label="URL",
        ),
    )
    # A configuration value for whether to validate the SSL certificate
    # of the Millenium Patron API server.
    verify_certificate: bool = FormField(
        True,
        form=ConfigurationFormItem(
            label="Certificate Verification",
            type=ConfigurationFormItemType.SELECT,
            options={
                "true": "Verify Certificate Normally (Required for production)",
                "false": "Ignore Certificate Problems (For temporary testing only)",
            },
        ),
    )
    # The field to use when seeing which values of MBLOCK[p56] mean a patron
    # is blocked. By default, any value other than '-' indicates a block.
    block_types: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Block Types",
            description="Values of MBLOCK[p56] which mean a patron is blocked. By default, any value other "
            "than '-' indicates a block.",
        ),
    )
    # Identifiers that contain any of these strings are ignored when
    # finding the "correct" identifier in a patron's record, even if
    # it means they end up with no identifier at all.
    identifier_blacklist: List[str] = FormField(
        [],
        form=ConfigurationFormItem(
            label="Identifier Blacklist",
            description="Identifiers containing any of these strings are ignored when finding the 'correct' "
            "identifier for a patron's record, even if it means they end up with no identifier at all. "
            'If librarians invalidate library cards by adding strings like "EXPIRED" or "INVALID" '
            "on to the beginning of the card number, put those strings here so the Circulation Manager "
            "knows they do not represent real card numbers.",
            type=ConfigurationFormItemType.LIST,
        ),
    )
    # The field to use when validating a patron's credential.
    authentication_mode: AuthenticationMode = FormField(
        AuthenticationMode.PIN,
        form=ConfigurationFormItem(
            label="Authentication Mode",
            type=ConfigurationFormItemType.SELECT,
            options={
                AuthenticationMode.PIN: "PIN",
                AuthenticationMode.FAMILY_NAME: "Family Name",
            },
        ),
        alias="auth_mode",
    )
    neighborhood_mode: NeighborhoodMode = FormField(
        NeighborhoodMode.DISABLED,
        form=ConfigurationFormItem(
            label="Patron neighborhood field",
            description="It's sometimes possible to guess a patron's neighborhood from their ILS record. "
            "You can use this when analyzing circulation activity by neighborhood. If you don't need to do "
            "this, it's better for patron privacy to disable this feature.",
            type=ConfigurationFormItemType.SELECT,
            options={
                NeighborhoodMode.DISABLED: "Disable this feature",
                NeighborhoodMode.HOME_BRANCH: "Patron's home library branch is their neighborhood.",
                NeighborhoodMode.POSTAL_CODE: "Patron's postal code is their neighborhood.",
            },
        ),
    )
    # The option that defines which field will be used for the patron identifier.
    # Defaults to the barcode field ("pb").
    field_used_as_patron_identifier: str = FormField(
        "pb",
        form=ConfigurationFormItem(
            label="Field for patron identifier",
            description="The name of the field used as a patron identifier. Typically, this will be the "
            "<i>barcode</i> field which has code <tt>pb</tt>. Some systems, however, are configured to "
            "use a different field (such as the <i>username</i> field, which has code <tt>pu</tt>).",
            required=True,
        ),
    )
    use_post_requests: bool = FormField(
        False,
        form=ConfigurationFormItem(
            label="Use POST for requests",
            description="Whether to use POST (instead of GET) HTTP requests. If this is a Virtual Library Card "
            "integration, using POST will improve the security of this integration and is the recommended "
            "setting. Otherwise, do not use POST, as it is NOT compatible with other Millenium integrations.",
            type=ConfigurationFormItemType.SELECT,
            options={
                "true": "True",
                "false": "False",
            },
        ),
    )


class MilleniumPatronLibrarySettings(BasicAuthProviderLibrarySettings):
    library_identifier_field: str = FormField(
        "barcode",
        form=ConfigurationFormItem(
            label="Library Identifier Field",
            description="This is the field on the patron record that the <em>Library Identifier Restriction "
            "Type</em> is applied to. The option 'barcode' matches the users barcode, other "
            "values are pulled directly from the patron record for example: 'P TYPE[p47]'. "
            "This value is not used if <em>Library Identifier Restriction Type</em> "
            "is set to 'No restriction'.",
        ),
    )


class MilleniumPatronAPI(BasicAuthenticationProvider, XMLParser):
    @classmethod
    def label(cls) -> str:
        return "Millenium"

    @classmethod
    def description(cls) -> str:
        return _("III Millenium Patron API")

    @classmethod
    def settings_class(cls) -> Type[MilleniumPatronSettings]:
        return MilleniumPatronSettings

    @classmethod
    def library_settings_class(cls) -> Type[MilleniumPatronLibrarySettings]:
        return MilleniumPatronLibrarySettings

    ERROR_MESSAGE_FIELD = "ERRMSG"
    RECORD_NUMBER_FIELD = "p81"  # e.g., "RECORD #[p81]"
    PATRON_TYPE_FIELD = "p47"  # e.g., "P TYPE[p47]"
    EXPIRATION_FIELD = "p43"  # e.g., "EXP DATE[p43]"
    HOME_BRANCH_FIELD = "p53"  # e.g., "HOME LIBR[p53]"
    ADDRESS_FIELD = "pa"  # e.g., "ADDRESS[pa]"
    BARCODE_FIELD = "pb"  # e.g., "P BARCODE[pb]"
    USERNAME_FIELD = "pu"  # e.g., "UNIV ID[pu]"
    FINES_FIELD = "p96"  # e.g., "MONEY OWED[p96]"
    BLOCK_FIELD = "p56"  # e.g., "MBLOCK[p56]"
    PERSONAL_NAME_FIELD = "pn"  # e.g., "PATRN NAME[pn]"
    EMAIL_ADDRESS_FIELD = "pz"  # e.g., "EMAIL ADDR[pz]"
    NOTE_FIELD = "px"  # e.g., "NOTE[px]"
    EXPIRATION_DATE_FORMAT = "%m-%d-%y"

    MULTIVALUE_FIELDS = {NOTE_FIELD, BARCODE_FIELD}

    # The following regex will match a field name of the form `<label>[<code>]`
    # with a group for the code. E.g., "P TYPE[p47]" -> "p47".
    FIELD_CODE_REGEX = re.compile(r".*\[(.*)\]")

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: MilleniumPatronSettings,
        library_settings: MilleniumPatronLibrarySettings,
        analytics: Optional[Analytics] = None,
    ):
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )
        url = str(settings.url)
        if not url.endswith("/"):
            url = url + "/"
        self.root = url
        self.verify_certificate = settings.verify_certificate
        self.parser = etree.HTMLParser()

        # In a Sierra ILS, a patron may have a large number of
        # identifiers, some of which are not real library cards. A
        # blacklist allows us to exclude certain types of identifiers
        # from being considered as library cards.
        self.blacklist = [re.compile(x, re.I) for x in settings.identifier_blacklist]

        self.auth_mode = settings.authentication_mode
        self.block_types = settings.block_types
        self.neighborhood_mode = settings.neighborhood_mode
        self.field_used_as_patron_identifier = settings.field_used_as_patron_identifier
        self.use_post = settings.use_post_requests

    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def _request(self, path):
        """Make an HTTP request and parse the response."""

    def remote_authenticate(
        self, username: Optional[str], password: Optional[str]
    ) -> Optional[PatronData]:
        """Does the Millenium Patron API approve of these credentials?

        :return: False if the credentials are invalid. If they are
            valid, a PatronData that serves only to indicate which
            authorization identifier the patron prefers.
        """
        if not username:
            return None

        if not self.collects_password:
            # We don't even look at the password. If the patron exists, they
            # are authenticated.
            patrondata = self.remote_patron_lookup(username)
            if not patrondata:
                return None
            return patrondata

        if self.auth_mode == AuthenticationMode.PIN:
            return self._remote_authenticate_pintest(
                username=username, password=password
            )
        elif self.auth_mode == AuthenticationMode.FAMILY_NAME:
            # Patrons are authenticated by their family name.
            patrondata = self.remote_patron_lookup(username)
            if not patrondata:
                # The patron doesn't even exist.
                return None

            # The patron exists; but do the last names match?
            if self.family_name_match(patrondata.personal_name, password):
                # Since this is a complete PatronData, we'll be able
                # to update their account without making a separate
                # call to /dump.
                return patrondata
        return None

    def _remote_authenticate_pintest(
        self, username: str, password: Optional[str]
    ) -> Optional[PatronData]:
        # Patrons are authenticated with a secret PIN.
        #
        # The PIN is URL-encoded. The username is not: as far as
        # we can tell Millenium Patron doesn't even try to decode
        # it.
        quoted_password = parse.quote(password, safe="") if password else password

        result: dict = {}
        if self.use_post:
            data = f"number={username}&pin={quoted_password}"
            path = "pintest"
            url = self.root + path
            response = self.request_post(
                url,
                data=data,
                headers={"content-type": "application/x-www-form-urlencoded"},
            )
            result = dict(self._extract_text_nodes(response.content))
        else:
            path = "%(barcode)s/%(pin)s/pintest" % dict(
                barcode=username, pin=quoted_password
            )
            url = self.root + path
            response = self.request(url)
            result = dict(self._extract_text_nodes(response.content))

        if result.get("RETCOD") == "0":
            return PatronData(authorization_identifier=username, complete=False)

        return None

    @classmethod
    def family_name_match(self, actual_name, supposed_family_name):
        """Does `supposed_family_name` match `actual_name`?"""
        if actual_name is None or supposed_family_name is None:
            return False
        if actual_name.find(",") != -1:
            actual_family_name = actual_name.split(",")[0]
        else:
            actual_name_split = actual_name.split(" ")
            actual_family_name = actual_name_split[-1]
        if actual_family_name.upper() == supposed_family_name.upper():
            return True
        return False

    def remote_patron_lookup(
        self, patron_or_patrondata_or_identifier: Union[PatronData, Patron, str]
    ) -> Optional[PatronData]:
        if isinstance(patron_or_patrondata_or_identifier, str):
            identifier = patron_or_patrondata_or_identifier
        else:
            if not patron_or_patrondata_or_identifier.authorization_identifier:
                return None
            identifier = patron_or_patrondata_or_identifier.authorization_identifier
        """Look up patron information for the given identifier."""
        path = "%(barcode)s/dump" % dict(barcode=identifier)
        url = self.root + path
        response = self.request(url)
        return self.patron_dump_to_patrondata(identifier, response.content)

    # End implementation of BasicAuthenticationProvider abstract
    # methods.

    def request(self, url, *args, **kwargs):
        """Actually make an HTTP request. This method exists only so the mock
        can override it.
        """
        self._update_request_kwargs(kwargs)
        return HTTP.request_with_timeout("GET", url, *args, **kwargs)

    def request_post(self, url, *args, **kwargs):
        """Actually make an HTTP request. This method exists only so the mock
        can override it.
        """
        self._update_request_kwargs(kwargs)
        return HTTP.request_with_timeout("POST", url, *args, **kwargs)

    def _update_request_kwargs(self, kwargs):
        """Modify the kwargs to HTTP.request_with_timeout to reflect the API
        configuration, in a testable way.
        """
        kwargs["verify"] = self.verify_certificate

    @classmethod
    def _patron_block_reason(cls, block_types, mblock_value):
        """Turn a value of the MBLOCK[56] field into a block type."""

        if block_types and mblock_value in block_types:
            # We are looking for a specific value, and we found it
            return PatronData.UNKNOWN_BLOCK

        if not block_types:
            # Apply the default rules.
            if not mblock_value or mblock_value.strip() in ("", "-"):
                # This patron is not blocked at all.
                return PatronData.NO_VALUE
            else:
                # This patron is blocked for an unknown reason.
                return PatronData.UNKNOWN_BLOCK

        # We have specific types that mean the patron is blocked.
        if mblock_value in block_types:
            # The patron has one of those types. They are blocked.
            return PatronData.UNKNOWN_BLOCK

        # The patron does not have one of those types, so is not blocked.
        return PatronData.NO_VALUE

    @classmethod
    def _code_from_field(cls, field_name: Optional[str]) -> Optional[str]:
        """Convert a Millenium property key to its code.

        A field name may comprise a label and a code or just a code.

        If the field name is of the form "<label>[<code>]" (e.g., "P TYPE[p47]"),
        return "<code>" (e.g., "p47"). Otherwise, return the original field name.
        """
        if field_name is None:
            return None

        match = cls.FIELD_CODE_REGEX.match(field_name)
        return match.groups()[0] if match is not None else field_name

    def _is_blacklisted(self, identifier: str) -> bool:
        # This identifier contains a blacklisted
        # string. Ignore it, even if this means the patron
        # ends up with no identifier whatsoever.
        return any(x.search(identifier) for x in self.blacklist)

    def _is_patron_identifier_field(self, k: str) -> bool:
        return k == self.field_used_as_patron_identifier

    def patron_dump_to_patrondata(self, current_identifier, content):
        """Convert an HTML patron dump to a PatronData object.

        :param current_identifier: Either the authorization identifier
            the patron just logged in with, or the one currently
            associated with their Patron record. Keeping track of this
            ensures we don't change a patron's preferred authorization
            identifier out from under them.

        :param content: The HTML document containing the patron dump.
        """
        # If we don't see these fields, erase any previous value
        # rather than leaving the old value in place. This shouldn't
        # happen (unless the expiration date changes to an invalid
        # date), but just to be safe.
        permanent_id = PatronData.NO_VALUE
        username = authorization_expires = personal_name = PatronData.NO_VALUE
        email_address = fines = external_type = PatronData.NO_VALUE
        block_reason = PatronData.NO_VALUE
        neighborhood = PatronData.NO_VALUE

        potential_identifiers = []
        for f, v in self._extract_text_nodes(content):
            k = self._code_from_field(f)

            # Check to see if the key should be treated as if it is a patron identifier.
            # This is dependent on a configuration setting. The key will also have a
            # chance to be treated as a different kind of field below (for example, if
            # the configuration says that the username field 'pu' should be treated as
            # a patron identifier, we _also_ want to treat it as a username below; both
            # classifications should apply!
            if self._is_patron_identifier_field(k):
                if self._is_blacklisted(v):
                    continue
                # We'll figure out which barcode is the 'right' one
                # later.
                potential_identifiers.append(v)
                # The millenium API doesn't care about spaces, so we add
                # a version of the barcode without spaces to our identifiers
                # list as well.
                if " " in v:
                    potential_identifiers.append(v.replace(" ", ""))

            # Handle all the other interpretations for fields.
            if k == self.USERNAME_FIELD:
                if self._is_blacklisted(v):
                    continue
                username = v
            elif k == self.RECORD_NUMBER_FIELD:
                permanent_id = v
            elif k == self.PERSONAL_NAME_FIELD:
                personal_name = v
            elif k == self.EMAIL_ADDRESS_FIELD:
                email_address = v
            elif k == self.FINES_FIELD:
                try:
                    fines = MoneyUtility.parse(v)
                except ValueError:
                    self.log.warning(
                        'Malformed fine amount for patron: "%s". Treating as no fines.'
                    )
                    fines = Money("0", "USD")
            elif k == self.BLOCK_FIELD:
                block_reason = self._patron_block_reason(self.block_types, v)
            elif k == self.EXPIRATION_FIELD:
                try:
                    # Parse the expiration date according to server local
                    # time, not UTC.
                    expires_local = datetime.datetime.strptime(
                        v, self.EXPIRATION_DATE_FORMAT
                    ).replace(tzinfo=dateutil.tz.tzlocal())
                    expires_local = expires_local.date()
                    authorization_expires = expires_local
                except ValueError:
                    self.log.warning(
                        'Malformed expiration date for patron: "%s". Treating as unexpirable.',
                        v,
                    )
            elif k == self.PATRON_TYPE_FIELD:
                external_type = v
            elif (
                k == self.HOME_BRANCH_FIELD
                and self.neighborhood_mode == NeighborhoodMode.HOME_BRANCH
            ):
                neighborhood = v.strip()
            elif (
                k == self.ADDRESS_FIELD
                and self.neighborhood_mode == NeighborhoodMode.POSTAL_CODE
            ):
                neighborhood = self.extract_postal_code(v)
            elif k == self.ERROR_MESSAGE_FIELD:
                # An error has occurred. Most likely the patron lookup
                # failed.
                return None

        # Set the library identifier field
        library_identifier_field_code = self._code_from_field(
            self.library_identifier_field
        )
        for k, v in self._extract_text_nodes(content):
            code = self._code_from_field(k)
            if code == library_identifier_field_code:
                library_identifier = v.strip()
                break
        else:
            library_identifier = None

        # We may now have multiple authorization
        # identifiers. PatronData expects the best authorization
        # identifier to show up first in the list.
        #
        # The last identifier in the list is probably the most recently
        # added one. In the absence of any other information, it's the
        # one we should choose.
        potential_identifiers.reverse()

        authorization_identifiers = potential_identifiers
        if not authorization_identifiers:
            authorization_identifiers = PatronData.NO_VALUE
        elif current_identifier in authorization_identifiers:
            # Don't rock the boat. The patron is used to using this
            # identifier and there's no need to change it. Move the
            # currently used identifier to the front of the list.
            authorization_identifiers.remove(current_identifier)
            authorization_identifiers.insert(0, current_identifier)

        data = PatronData(
            permanent_id=permanent_id,
            authorization_identifier=authorization_identifiers,
            username=username,
            personal_name=personal_name,
            email_address=email_address,
            authorization_expires=authorization_expires,
            external_type=external_type,
            fines=fines,
            block_reason=block_reason,
            library_identifier=library_identifier,
            neighborhood=neighborhood,
            # We must cache neighborhood information in the patron's
            # database record because syncing with the ILS is so
            # expensive.
            cached_neighborhood=neighborhood,
            complete=True,
        )
        return data

    def _extract_text_nodes(self, content):
        """Parse the HTML representations sent by the Millenium Patron API."""
        if isinstance(content, bytes):
            content = content.decode("utf8")
        for line in content.split("\n"):
            if line.startswith("<HTML><BODY>"):
                line = line[12:]
            if not line.endswith("<BR>"):
                continue
            kv = line[:-4]
            if not "=" in kv:
                # This shouldn't happen, but there's no need to crash.
                self.log.warning("Unexpected line in patron dump: %s", line)
                continue
            yield kv.split("=", 1)

    # A number of regular expressions for finding postal codes in
    # freeform addresses, with more reliable techniques at the front.
    POSTAL_CODE_RES = [
        re.compile(x)
        for x in [
            "[^0-9]([0-9]{5})-[0-9]{4}$",  # ZIP+4 at end
            "[^0-9]([0-9]{5})$",  # ZIP at end
            ".*[^0-9]([0-9]{5})-[0-9]{4}[^0-9]",  # ZIP+4 as close to end as possible without being at the end
            ".*[^0-9]([0-9]{5})[^0-9]",  # ZIP as close to end as possible without being at the end
        ]
    ]

    @classmethod
    def extract_postal_code(cls, address):
        """Try to extract a postal code from an address."""
        for r in cls.POSTAL_CODE_RES:
            match = r.search(address)
            if match:
                return match.groups()[0]
        return None


class MockMilleniumPatronAPI(MilleniumPatronAPI):

    """This mocks the API on a higher level than the HTTP level.

    It is not used in the tests of the MilleniumPatronAPI class.  It
    is used in the Adobe Vendor ID tests but maybe it shouldn't.
    """

    # For expiration dates we're using UTC instead of local time for
    # convenience; the difference doesn't matter because the dates in
    # question are at least 10 days away from the current date.

    # This user's card has expired.
    user1 = PatronData(
        permanent_id="12345",
        authorization_identifier="0",
        username="alice",
        authorization_expires=datetime_utc(2015, 4, 1),
    )

    # This user's card still has ten days on it.
    the_future = utc_now() + datetime.timedelta(days=10)
    user2 = PatronData(
        permanent_id="67890",
        authorization_identifier="5",
        username="bob",
        authorization_expires=the_future,
    )

    users = [user1, user2]

    def __init__(self):
        pass

    def remote_authenticate(self, barcode, pin):
        """A barcode that's 14 digits long is treated as valid,
        no matter which PIN is used.

        That's so real barcode/PIN combos can be passed through to
        third parties.

        Otherwise, valid test PIN is the first character of the barcode
        repeated four times.

        """
        u = self.dump(barcode)
        if "ERRNUM" in u:
            return False
        return len(barcode) == 14 or pin == barcode[0] * 4

    def remote_patron_lookup(self, patron_or_patrondata):
        # We have a couple custom barcodes.
        look_for = patron_or_patrondata.authorization_identifier
        for u in self.users:
            if u.authorization_identifier == look_for:
                return u
        return None
