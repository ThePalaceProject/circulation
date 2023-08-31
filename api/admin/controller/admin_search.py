from __future__ import annotations

from typing import List, Tuple

import flask
from sqlalchemy import func, or_

from api.admin.controller.base import AdminController
from core.model import (
    Classification,
    DataSource,
    Edition,
    Genre,
    Library,
    LicensePool,
    Subject,
)
from core.util import LanguageCodes
from core.util.cache import memoize


class AdminSearchController(AdminController):
    """APIs for the admin search pages
    Eg. Lists Creation
    """

    def search_field_values(self) -> dict:
        """Enumerate the possible values for the search fields with counts
        - Audience
        - Distributor
        - Genre
        - Language
        - Publisher
        - Subject
        """
        library: Library = flask.request.library  # type: ignore
        collection_ids = [coll.id for coll in library.collections if coll.id]
        return self._search_field_values_cached(collection_ids)

    @classmethod
    def _unzip(cls, values: List[Tuple[str, int]]) -> dict:
        """Covert a list of tuples to a {value0: value1} dictionary"""
        return {a[0]: a[1] for a in values if type(a[0]) is str}

    # 1 hour in-memory cache
    @memoize(ttls=3600)
    def _search_field_values_cached(self, collection_ids: List[int]) -> dict:
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
            classification_query.group_by(Subject.name).values(
                func.distinct(Subject.name), func.count(Subject.name)
            )
        )
        subjects = self._unzip(subjects_list)

        audiences_list = list(
            classification_query.group_by(Subject.audience).values(
                func.distinct(Subject.audience), func.count(Subject.audience)
            )
        )
        audiences = self._unzip(audiences_list)

        genres_list = list(
            classification_query.join(Subject.genre)
            .group_by(Genre.name)
            .values(func.distinct(Genre.name), func.count(Genre.name))
        )
        genres = self._unzip(genres_list)

        distributors_list = list(
            editions_query.join(Edition.data_source)
            .group_by(DataSource.name)
            .values(func.distinct(DataSource.name), func.count(DataSource.name))
        )
        distributors = self._unzip(distributors_list)

        languages_list = list(
            editions_query.group_by(Edition.language).values(
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
            editions_query.group_by(Edition.publisher).values(
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
