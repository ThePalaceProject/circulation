import datetime
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Mapping
from typing import Annotated, Any, Self

from requests import Response
from sqlalchemy.engine import Row
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import and_, join, or_, select

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.base import HasLibraryIntegrationConfiguration
from palace.manager.integration.goals import Goals
from palace.manager.integration.metadata.base import (
    MetadataService,
    MetadataServiceSettings,
)
from palace.manager.integration.settings import (
    BaseSettings,
    FormMetadata,
)
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Equivalency, Identifier
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.resource import (
    HttpResponseTuple,
    Representation,
)
from palace.manager.util.http.http import HTTP
from palace.manager.util.log import LoggerMixin


class NoveListApiSettings(MetadataServiceSettings):
    """Settings for the NoveList API"""

    username: Annotated[
        str,
        FormMetadata(
            label="Profile",
        ),
    ]
    password: Annotated[
        str,
        FormMetadata(
            label="Password",
        ),
    ]


class NoveListApiLibrarySettings(BaseSettings): ...


class NoveListAPI(
    MetadataService[NoveListApiSettings],
    HasLibraryIntegrationConfiguration[NoveListApiSettings, NoveListApiLibrarySettings],
    LoggerMixin,
):
    # Hardcoded authentication key used as a Header for calling the NoveList
    # Collections API. It identifies the client, and lets NoveList know that
    # SimplyE is making the requests.
    # TODO: This is leftover from before the fork with SimplyE. We should probably
    #  get a new API key for Palace and use that instead.
    AUTHORIZED_IDENTIFIER = "62521fa1-bdbb-4939-84aa-aee2a52c8d59"

    version = "2.2"
    NO_ISBN_EQUIVALENCY = "No clear ISBN equivalency: %r"

    # While the NoveList API doesn't require parameters to be passed via URL,
    # the Representation object needs a unique URL to return the proper data
    # from the database.
    QUERY_ENDPOINT = (
        "https://novselect.ebscohost.com/Data/ContentByQuery?"
        "ISBN=%(ISBN)s&ClientIdentifier=%(ClientIdentifier)s&version=%(version)s"
    )
    COLLECTION_DATA_API = "http://www.noveListcollectiondata.com/api/collections"
    AUTH_PARAMS = "&profile=%(profile)s&password=%(password)s"
    MAX_REPRESENTATION_AGE = 7 * 24 * 60 * 60  # one week

    medium_to_book_format_type_values = {
        Edition.BOOK_MEDIUM: "EBook",
        Edition.AUDIO_MEDIUM: "Audiobook",
    }

    @classmethod
    def from_config(cls, library: Library) -> Self:
        settings = cls.values(library)
        if not settings:
            raise CannotLoadConfiguration(
                "No NoveList integration configured for library (%s)."
                % library.short_name
            )

        _db = Session.object_session(library)
        return cls(_db, settings)

    @classmethod
    def integration(cls, library: Library) -> IntegrationConfiguration | None:
        _db = Session.object_session(library)
        query = select(IntegrationConfiguration).where(
            IntegrationConfiguration.goal == Goals.METADATA_GOAL,
            IntegrationConfiguration.libraries.contains(library),
            IntegrationConfiguration.protocol.in_(cls.protocols()),
        )

        return _db.execute(query).scalar_one_or_none()

    @classmethod
    def values(cls, library: Library) -> NoveListApiSettings | None:
        integration = cls.integration(library)
        if not integration:
            return None

        return cls.settings_load(integration)

    @classmethod
    def is_configured_db_check(cls, library: Library) -> bool:
        """Checks if a NoveList integration exists and is configured for the given library.

        Note: This method performs a database query to find the relevant
        integration. It should not be called repeatedly in performance-sensitive
        code without considering caching strategies as appropriate.

        :param library: The Library to check for a NoveList configuration.
        :return: True if a NoveList integration is configured, False otherwise.
        """
        integration = cls.integration(library)
        return integration is not None

    @classmethod
    def label(cls) -> str:
        return "Novelist API"

    @classmethod
    def description(cls) -> str:
        return ""

    @classmethod
    def settings_class(cls) -> type[NoveListApiSettings]:
        return NoveListApiSettings

    @classmethod
    def library_settings_class(cls) -> type[NoveListApiLibrarySettings]:
        return NoveListApiLibrarySettings

    @classmethod
    def multiple_services_allowed(cls) -> bool:
        return True

    def __init__(self, _db: Session, settings: NoveListApiSettings) -> None:
        self._db = _db
        self.profile = settings.username
        self.password = settings.password

    @property
    def source(self) -> DataSource:
        return DataSource.lookup(self._db, DataSource.NOVELIST, autocreate=True)

    def _lookup_recommendations_equivalent_isbns(
        self, identifier: Identifier
    ) -> list[IdentifierData]:
        """Finds NoveList recommendations for all ISBNs equivalent to an identifier.

        :return: List of IdentifierData objects for the recommended books.
        """
        license_sources = DataSource.license_sources_for(self._db, identifier)

        # Find strong ISBN equivalents.
        isbns = list()
        for license_source in license_sources:
            isbns += [
                eq.output
                for eq in identifier.equivalencies
                if (
                    eq.data_source == license_source
                    and eq.strength == 1
                    and eq.output.type == Identifier.ISBN
                )
            ]

        if not isbns:
            self.log.warning(
                (
                    "Identifiers without an ISBN equivalent can't"
                    "be looked up with NoveList: %r"
                ),
                identifier,
            )
            return []

        # Look up recommendations for all equivalent ISBNs.
        lookups = list()
        for isbn in isbns:
            novelist_id, recs = self._lookup_recommendations_isbn(
                isbn.identifier, isbn.urn
            )
            if novelist_id:
                lookups.append((novelist_id, recs))

        if not lookups:
            self.log.warning(
                (
                    "No NoveList metadata found for Identifiers without an ISBN"
                    "equivalent can't be looked up with NoveList: %r"
                ),
                identifier,
            )
            return []

        best_recommendations, confidence = self._choose_best_recommendations(
            lookups, identifier
        )
        if round(confidence, 2) < 0.5:
            self.log.warning(self.NO_ISBN_EQUIVALENCY, identifier)
            return []

        return best_recommendations

    @classmethod
    def _confirm_same_identifier(
        cls, recommendation_lookups: list[tuple[IdentifierData, list[IdentifierData]]]
    ) -> bool:
        """Ensures that all recommendation_lookups have the same NoveList ID"""

        novelist_ids = {novelist_id for novelist_id, _ in recommendation_lookups}
        return len(novelist_ids) == 1

    def _choose_best_recommendations(
        self,
        recommendation_lookups: list[tuple[IdentifierData, list[IdentifierData]]],
        identifier: Identifier,
    ) -> tuple[list[IdentifierData], float]:
        """
        Chooses the most reliable set of book recommendations when multiple NoveList IDs are found.

        When an ISBN has multiple equivalent identifiers that map to different NoveList IDs,
        this method determines which set of recommendations to use by selecting the most
        frequently occurring NoveList ID. It returns both the chosen recommendations and
        a confidence score based on how dominant that ID is in the results.
        """
        if self._confirm_same_identifier(recommendation_lookups):
            # Metadata with the same NoveList ID will be identical. Take one.
            return recommendation_lookups[0][1], 1.0

        # One or more of the equivalents did not return the same NoveList work
        self.log.warning("%r has inaccurate ISBN equivalents", identifier)
        counter: Counter[IdentifierData] = Counter()
        for novelist_id, _ in recommendation_lookups:
            counter[novelist_id] += 1

        [(target_identifier, most_amount), (ignore, secondmost)] = counter.most_common(
            2
        )
        if most_amount == secondmost:
            # The counts are the same, and neither can be trusted.
            self.log.warning(self.NO_ISBN_EQUIVALENCY, identifier)
            return [], 0
        confidence = most_amount / float(len(recommendation_lookups))
        target = [
            recs
            for novelist_id, recs in recommendation_lookups
            if novelist_id == target_identifier
        ]
        return target[0], confidence

    def lookup_recommendations(self, identifier: Identifier) -> list[IdentifierData]:
        """Requests NoveList recommendations for a given identifier.

        :return: List of IdentifierData objects for the recommended books.
        """
        if identifier.type != Identifier.ISBN:
            return self._lookup_recommendations_equivalent_isbns(identifier)

        novelist_identifier, recs = self._lookup_recommendations_isbn(
            identifier.identifier, identifier.urn
        )

        return recs

    def _lookup_recommendations_isbn(
        self, isbn: str, client_identifier: str
    ) -> tuple[IdentifierData | None, list[IdentifierData]]:
        params = dict(
            ClientIdentifier=client_identifier,
            ISBN=isbn,
            version=self.version,
            profile=self.profile,
            password=self.password,
        )
        scrubbed_url = str(self.scrubbed_url(params))

        url = self.build_query_url(params)
        self.log.debug("NoveList lookup: %s", url)

        # We want to make an HTTP request for `url` but cache the
        # result under `scrubbed_url`. Define a 'URL normalization'
        # function that always returns `scrubbed_url`.
        def normalized_url(original: str) -> str:
            return scrubbed_url

        representation, from_cache = Representation.post(
            _db=self._db,
            url=str(url),
            data="",
            max_age=self.MAX_REPRESENTATION_AGE,
            response_reviewer=self.review_response,
            url_normalizer=normalized_url,
        )

        # Commit to the database immediately to reduce the chance
        # that some other incoming request will try to create a
        # duplicate Representation and crash.
        self._db.commit()

        return self._lookup_info_representation_to_recommendations(representation)

    @classmethod
    def review_response(cls, response: HttpResponseTuple) -> None:
        """Performs NoveList-specific error review of the request response"""
        status_code, headers, content = response
        if status_code == 403:
            raise Exception("Invalid NoveList credentials")
        if content.startswith(b'"Missing'):
            raise Exception("Invalid NoveList parameters: %s" % content.decode("utf-8"))

    @classmethod
    def scrubbed_url(cls, params: Mapping[str, str]) -> str:
        """Removes authentication details from cached Representation.url"""
        return cls.build_query_url(params, include_auth=False)

    @classmethod
    def build_query_url(
        cls, params: Mapping[str, str], include_auth: bool = True
    ) -> str:
        """Builds a unique and url-encoded query endpoint"""
        url = cls.QUERY_ENDPOINT
        if include_auth:
            url += cls.AUTH_PARAMS

        urlencoded_params = dict()
        for name, value in list(params.items()):
            urlencoded_params[name] = urllib.parse.quote(value)
        return url % urlencoded_params

    def _lookup_info_representation_to_recommendations(
        self, lookup_representation: Representation
    ) -> tuple[IdentifierData | None, list[IdentifierData]]:
        """Transforms a NoveList JSON representation into a tuple containing
        the NoveList ID and a list of recommended ISBNs."""

        if not lookup_representation.content:
            return None, []

        lookup_info = json.loads(lookup_representation.content)
        book_info = lookup_info.get("TitleInfo")
        novelist_identifier = book_info.get("ui") if book_info else None
        if not book_info or not novelist_identifier:
            # NoveList didn't know the ISBN.
            return None, []

        novelist_identifier_data = IdentifierData(
            type=Identifier.NOVELIST_ID, identifier=novelist_identifier
        )

        # Get the equivalent ISBN identifiers.
        book_identifiers = set(self._extract_isbns(book_info))

        # Extract similar content if it is available.
        similar_titles = lookup_info.get("FeatureContent", {}).get("SimilarTitles", {})

        recommendations = self._get_recommendations(similar_titles, book_identifiers)

        return novelist_identifier_data, recommendations

    @staticmethod
    def _extract_isbns(
        book_info: Mapping[str, Any], *, filter: set[IdentifierData] | None = None
    ) -> list[IdentifierData]:
        if filter is None:
            filter = set()
        isbns = []
        synonymous_ids = book_info.get("manifestations", [])
        for synonymous_id in synonymous_ids:
            isbn = synonymous_id.get("ISBN")
            if isbn:
                isbn_data = IdentifierData(type=Identifier.ISBN, identifier=isbn)
                if isbn_data not in filter:
                    isbns.append(isbn_data)

        return isbns

    def _get_recommendations(
        self,
        recommendations_info: Mapping[str, Any],
        book_identifiers: set[IdentifierData],
    ) -> list[IdentifierData]:
        recommendations = []
        related_books = recommendations_info.get("titles", [])
        related_books = [b for b in related_books if b.get("is_held_locally")]
        if related_books:
            for book_info in related_books:
                recommendations += self._extract_isbns(
                    book_info, filter=book_identifiers
                )

        return recommendations

    def get_items_from_query(self, library: Library) -> list[dict[str, str]]:
        """Gets identifiers and its related title, medium, and authors from the
        database.
        Keeps track of the current 'ISBN' identifier and current item object that
        is being processed. If the next ISBN being processed is new, the existing one
        gets added to the list of items. If the ISBN is the same, then we append
        the Author property since there are multiple contributors.

        :return: a list of Novelist objects to send
        """
        collectionList = [c.id for c in library.active_collections]

        LEFT_OUTER_JOIN = True
        i1 = aliased(Identifier)
        i2 = aliased(Identifier)
        roles = list(Contributor.AUTHOR_ROLES)
        roles.append(Contributor.Role.NARRATOR)

        isbnQuery = (
            select(
                i1.identifier,
                i1.type,
                i2.identifier,
                Edition.title,
                Edition.medium,
                Edition.published,
                Contribution.role,
                Contributor.sort_name,
                DataSource.name,
            )
            .select_from(
                join(LicensePool, i1, i1.id == LicensePool.identifier_id)
                .join(Equivalency, i1.id == Equivalency.input_id, LEFT_OUTER_JOIN)  # type: ignore[arg-type]
                .join(i2, Equivalency.output_id == i2.id, LEFT_OUTER_JOIN)
                .join(
                    Edition,  # type: ignore[arg-type]
                    or_(
                        Edition.primary_identifier_id == i1.id,
                        Edition.primary_identifier_id == i2.id,
                    ),
                )
                .join(Contribution, Edition.id == Contribution.edition_id)  # type: ignore[arg-type]
                .join(Contributor, Contribution.contributor_id == Contributor.id)  # type: ignore[arg-type]
                .join(DataSource, DataSource.id == LicensePool.data_source_id)  # type: ignore[arg-type]
            )
            .where(
                and_(
                    LicensePool.collection_id.in_(collectionList),
                    or_(i1.type == "ISBN", i2.type == "ISBN"),
                    or_(Contribution.role.in_(roles)),
                )
            )
            .order_by(i1.identifier, i2.identifier)
        )

        result = self._db.execute(isbnQuery)

        items = []
        newItem: dict[str, str] | None = None
        existingItem: dict[str, str] | None = None
        currentIdentifier: str | None = None

        # Loop through the query result. There's a need to keep track of the
        # previously processed object and the currently processed object because
        # the identifier could be the same. If it is, we update the data
        # object to send to Novelist.
        for item in result:
            if newItem:
                existingItem = newItem
            (
                currentIdentifier,
                existingItem,
                newItem,
                addItem,
            ) = self.create_item_object(item, currentIdentifier, existingItem)

            if addItem and existingItem:
                # The Role property isn't needed in the actual request.
                del existingItem["role"]
                items.append(existingItem)

        # For the case when there's only one item in `result`
        if newItem:
            del newItem["role"]
            items.append(newItem)

        return items

    def create_item_object(
        self,
        object: (
            Row | tuple[str, str, str, str, str, datetime.date, str, str, str] | None
        ),
        currentIdentifier: str | None,
        existingItem: dict[str, str] | None,
    ) -> tuple[str | None, dict[str, str] | None, dict[str, str] | None, bool]:
        """Returns a new item if the current identifier that was processed
        is not the same as the new object's ISBN being processed. If the new
        object's ISBN matches the current identifier, the previous object's
        Author property is updated.

        :param object: the current item object to process
        :param currentIdentifier: the current identifier to process
        :param existingItem: the previously processed item object

        :return: (
            current identifier,
            the existing object if available,
            a new object if the item wasn't found before,
            if the item is ready to the added to the list of books to send
            )
        """
        if not object:
            return (None, None, None, False)

        if object[1] == Identifier.ISBN:
            isbn = object[0]
        elif object[2] is not None:
            isbn = object[2]
        else:
            # We cannot find an ISBN for this work -- probably due to
            # a data error.
            return (None, None, None, False)

        roles = list(Contributor.AUTHOR_ROLES)
        roles.append(Contributor.Role.NARRATOR)

        role = object[6]
        author_or_narrator = object[7] if role in roles else ""
        distributor = object[8]

        # If there's no existing author value but we now get one, add it.
        # If the role is narrator and it's a new value
        # (i.e. no "narrator" was already added), then add the narrator.
        # If we encounter an existing ISBN and its role is "Primary Author",
        # then that value overrides the existing Author property.
        if isbn == currentIdentifier and existingItem:
            if not existingItem.get("author") and role in Contributor.AUTHOR_ROLES:
                existingItem["author"] = author_or_narrator
            if not existingItem.get("narrator") and role == Contributor.Role.NARRATOR:
                existingItem["narrator"] = author_or_narrator
            if role == Contributor.Role.PRIMARY_AUTHOR:
                existingItem["author"] = author_or_narrator
            existingItem["role"] = role

            # Always return False to keep processing the currentIdentifier until
            # we get a new ISBN to process. In that case, return and add all
            # the data we've accumulated for this object.
            return (currentIdentifier, existingItem, None, False)
        else:
            # If we encounter a new ISBN, we take whatever values are initially given.
            title = object[3]
            mediaType = self.medium_to_book_format_type_values.get(object[4], "")

            newItem = dict(
                isbn=isbn,
                title=title,
                mediaType=mediaType,
                role=role,
                distributor=distributor,
            )

            publicationDate = object[5]
            if publicationDate:
                publicationDateString = publicationDate.isoformat().replace("-", "")
                newItem["publicationDate"] = publicationDateString

            # If we are processing a new item and there is an existing item,
            # then we can add the existing item to the list and keep
            # the current new item for further data aggregation.
            addItem = True if existingItem else False
            if role in Contributor.AUTHOR_ROLES:
                newItem["author"] = author_or_narrator
            if role == Contributor.Role.NARRATOR:
                newItem["narrator"] = author_or_narrator

            return (isbn, existingItem, newItem, addItem)

    def put_items_novelist(self, library: Library) -> dict[str, Any] | None:
        items = self.get_items_from_query(library)

        content: dict[str, Any] | None = None
        if items:
            data = json.dumps(self.make_novelist_data_object(items))
            response = self.put(
                self.COLLECTION_DATA_API,
                {
                    "AuthorizedIdentifier": self.AUTHORIZED_IDENTIFIER,
                    "Content-Type": "application/json; charset=utf-8",
                },
                data=data,
            )
            if response.status_code == 200:
                content = json.loads(response.content)
                logging.info("Success from NoveList: %r", response.content)
            else:
                logging.error("Data sent was: %r", data)
                logging.error(
                    "Error %s from NoveList: %r", response.status_code, response.content
                )

        return content

    def make_novelist_data_object(self, items: list[dict[str, str]]) -> dict[str, Any]:
        return {
            "customer": f"{self.profile}:{self.password}",
            "records": items,
        }

    def put(self, url: str, headers: Mapping[str, str], **kwargs: Any) -> Response:
        # This might take a very long time -- disable the normal
        # timeout.
        kwargs["timeout"] = None
        response = HTTP.put_with_timeout(url, headers=headers, **kwargs)
        return response
