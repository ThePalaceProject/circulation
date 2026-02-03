from __future__ import annotations

from sqlalchemy import func, or_

from palace.manager.api.admin.controller.base import AdminController
from palace.manager.api.util.flask import get_request_library
from palace.manager.sqlalchemy.model.classification import (
    Classification,
    Genre,
    Subject,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.cache import memoize
from palace.manager.util.languages import LanguageCodes


class AdminSearchController(AdminController):
    """APIs for the admin search pages
    Eg. Lists Creation
    """

    def search_field_values(self) -> dict[str, dict[str, int]]:
        """Enumerate the possible values for the search fields with counts
        - Audience
        - Distributor
        - Genre
        - Language
        - Publisher
        - Subject
        """
        library = get_request_library()
        collection_ids = [coll.id for coll in library.active_collections if coll.id]
        return self._search_field_values_cached(collection_ids)

    @classmethod
    def _unzip(cls, values: list[tuple[str, int]]) -> dict[str, int]:
        """Covert a list of tuples to a {value0: value1} dictionary"""
        return {a[0]: a[1] for a in values if type(a[0]) is str}

    # 1 hour in-memory cache
    @memoize(ttls=3600)
    def _search_field_values_cached(
        self, collection_ids: list[int]
    ) -> dict[str, dict[str, int]]:
        licenses_filter = or_(
            LicensePool.open_access == True,
            LicensePool.licenses_owned != 0,
        )

        # Reusable queries
        classification_query = (
            self._db.query(Classification)
            .join(Classification.subject)
            .join(
                LicensePool, LicensePool.identifier_id == Classification.identifier_id
            )
            .filter(LicensePool.collection_id.in_(collection_ids), licenses_filter)
        )

        editions_query = (
            self._db.query(LicensePool)
            .join(LicensePool.presentation_edition)
            .filter(LicensePool.collection_id.in_(collection_ids), licenses_filter)
        )

        # Concrete values
        subjects_list = list(
            classification_query.group_by(Subject.name).with_entities(
                func.distinct(Subject.name), func.count(Subject.name)
            )
        )
        subjects = self._unzip(subjects_list)

        audiences_list = list(
            classification_query.group_by(Subject.audience).with_entities(
                func.distinct(Subject.audience), func.count(Subject.audience)
            )
        )
        audiences = self._unzip(audiences_list)

        genres_list = list(
            classification_query.join(Subject.genre)
            .group_by(Genre.name)
            .with_entities(func.distinct(Genre.name), func.count(Genre.name))
        )
        genres = self._unzip(genres_list)

        distributors_list = list(
            editions_query.join(Edition.data_source)
            .group_by(DataSource.name)
            .with_entities(func.distinct(DataSource.name), func.count(DataSource.name))
        )
        distributors = self._unzip(distributors_list)

        languages_list = list(
            editions_query.group_by(Edition.language).with_entities(
                func.distinct(Edition.language), func.count(Edition.language)
            )
        )
        converted_languages_list = []
        # We want full english names, not codes
        for name, num in languages_list:
            full_name_set = LanguageCodes.english_names.get(name, [name])
            # Language codes are an array of multiple choices, we only want one
            full_name = full_name_set[0] if len(full_name_set) > 0 else name
            converted_languages_list.append((full_name, num))
        languages = self._unzip(converted_languages_list)

        publishers_list = list(
            editions_query.group_by(Edition.publisher).with_entities(
                func.distinct(Edition.publisher), func.count(Edition.publisher)
            )
        )
        publishers = self._unzip(publishers_list)

        return {
            "subjects": subjects,
            "audiences": audiences,
            "genres": genres,
            "distributors": distributors,
            "languages": languages,
            "publishers": publishers,
        }
