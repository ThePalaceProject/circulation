import json
from collections.abc import Mapping
from typing import Any

import flask
from flask import Response
from flask_babel import lazy_gettext as _
from pydantic import TypeAdapter, ValidationError

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.model.work_editor import CustomListResponse
from palace.manager.api.admin.problem_details import (
    EROTICA_FOR_ADULTS_ONLY,
    GENRE_NOT_FOUND,
    INCOMPATIBLE_GENRE,
    INVALID_DATE_FORMAT,
    INVALID_EDIT,
    INVALID_RATING,
    INVALID_SERIES_POSITION,
    METADATA_REFRESH_FAILURE,
    METADATA_REFRESH_PENDING,
    MISSING_CUSTOM_LIST,
    UNKNOWN_LANGUAGE,
    UNKNOWN_MEDIUM,
    UNKNOWN_ROLE,
)
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.problem_details import (
    LIBRARY_NOT_FOUND,
    REMOTE_INTEGRATION_FAILED,
)
from palace.manager.api.util.flask import get_request_library
from palace.manager.core.classifier import NO_NUMBER, NO_VALUE, genres
from palace.manager.core.classifier.simplified import SimplifiedGenreClassifier
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.annotator.admin.suppressed import AdminSuppressedAnnotator
from palace.manager.sqlalchemy.model.classification import (
    Classification,
    Genre,
    Subject,
)
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import RightsStatus
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import create, get_one, get_one_or_create
from palace.manager.util.datetime_helpers import strptime_utc, utc_now
from palace.manager.util.languages import LanguageCodes
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class WorkController(CirculationManagerController, AdminPermissionsControllerMixin):
    STAFF_WEIGHT = 1000

    def details(
        self, identifier_type: str, identifier: str
    ) -> Response | ProblemDetail:
        """Return an OPDS entry with detailed information for admins.

        This includes relevant links for editing the book.

        :return: An OPDSEntryResponse
        """
        library = get_request_library()
        self.require_librarian(library)

        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        annotator = AdminSuppressedAnnotator(self.circulation, library)

        # single_entry returns an OPDSEntryResponse that will not be
        # cached, which is perfect. We want the admin interface
        # to update immediately when an admin makes a change.
        return OPDSAcquisitionFeed.entry_as_response(
            OPDSAcquisitionFeed.single_entry(work, annotator)
        )

    def roles(self) -> dict[str, Contributor.Role]:
        """Return a mapping from MARC codes to contributor roles."""
        # TODO: The admin interface only allows a subset of the roles
        # listed in model.py since it uses the OPDS representation of
        # the data, and some of the roles map to the same MARC code.
        CODES = Contributor.MARC_ROLE_CODES
        marc_to_role = dict()
        for role in [
            Contributor.Role.ACTOR,
            Contributor.Role.ADAPTER,
            Contributor.Role.AFTERWORD,
            Contributor.Role.ARTIST,
            Contributor.Role.ASSOCIATED,
            Contributor.Role.AUTHOR,
            Contributor.Role.COMPILER,
            Contributor.Role.COMPOSER,
            Contributor.Role.CONTRIBUTOR,
            Contributor.Role.COPYRIGHT_HOLDER,
            Contributor.Role.DESIGNER,
            Contributor.Role.DIRECTOR,
            Contributor.Role.EDITOR,
            Contributor.Role.ENGINEER,
            Contributor.Role.FOREWORD,
            Contributor.Role.ILLUSTRATOR,
            Contributor.Role.INTRODUCTION,
            Contributor.Role.LYRICIST,
            Contributor.Role.MUSICIAN,
            Contributor.Role.NARRATOR,
            Contributor.Role.PERFORMER,
            Contributor.Role.PHOTOGRAPHER,
            Contributor.Role.PRODUCER,
            Contributor.Role.TRANSCRIBER,
            Contributor.Role.TRANSLATOR,
        ]:
            marc_to_role[CODES[role]] = role
        return marc_to_role

    def languages(self) -> dict[str, list[str]]:
        """Return the supported language codes and their English names."""
        return LanguageCodes.english_names

    def media(self) -> dict[str, str]:
        """Return the supported media types for a work and their schema.org values."""
        return Edition.additional_type_to_medium

    def rights_status(self) -> dict[str, dict[str, str | bool]]:
        """Return the supported rights status values with their names and whether
        they are open access."""
        return {
            uri: dict(
                name=name,
                open_access=(uri in RightsStatus.OPEN_ACCESS),
                allows_derivatives=(uri in RightsStatus.ALLOWS_DERIVATIVES),
            )
            for uri, name in list(RightsStatus.NAMES.items())
        }

    def edit(self, identifier_type: str, identifier: str) -> Response | ProblemDetail:
        """Edit a work's metadata."""
        library = get_request_library()
        self.require_librarian(library)

        # TODO: It would be nice to use the metadata layer for this, but
        # this code handles empty values differently than other metadata
        # sources. When a staff member deletes a value, that indicates
        # they think it should be empty. This needs to be indicated in the
        # db so that it can overrule other data sources that set a value,
        # unlike other sources which set empty fields to None.

        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        changed = False

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        if staff_data_source is None:
            self._db.rollback()
            return INVALID_INPUT
        assert work.presentation_edition is not None
        primary_identifier = work.presentation_edition.primary_identifier
        staff_edition, is_new = get_one_or_create(
            self._db,
            Edition,
            primary_identifier_id=primary_identifier.id,
            data_source_id=staff_data_source.id,
        )
        self._db.expire(primary_identifier)

        new_title = flask.request.form.get("title")
        if new_title and work.title != new_title:
            staff_edition.title = str(new_title)
            changed = True

        new_subtitle = flask.request.form.get("subtitle")
        if work.subtitle != new_subtitle:
            if work.subtitle and not new_subtitle:
                new_subtitle = NO_VALUE
            staff_edition.subtitle = str(new_subtitle)
            changed = True

        # The form data includes roles and names for contributors in the same order.
        new_contributor_roles = flask.request.form.getlist("contributor-role")
        new_contributor_names = [
            str(n) for n in flask.request.form.getlist("contributor-name")
        ]
        # The first author in the form is considered the primary author, even
        # though there's no separate MARC code for that.
        for i, role in enumerate(new_contributor_roles):
            if role == Contributor.Role.AUTHOR:
                new_contributor_roles[i] = Contributor.Role.PRIMARY_AUTHOR
                break
        roles_and_names = list(zip(new_contributor_roles, new_contributor_names))

        # Remove any contributions that weren't in the form, and remove contributions
        # that already exist from the list so they won't be added again.
        deleted_contributions = False
        for contribution in staff_edition.contributions:
            display_name = contribution.contributor.display_name or ""
            if (contribution.role, display_name) not in roles_and_names:
                self._db.delete(contribution)
                deleted_contributions = True
                changed = True
            else:
                roles_and_names.remove((contribution.role, display_name))
        if deleted_contributions:
            # Ensure the staff edition's contributions are up-to-date when
            # calculating the presentation edition later.
            self._db.refresh(staff_edition)

        # Any remaining roles and names are new contributions.
        for role, name in roles_and_names:
            # There may be one extra role at the end from the input for
            # adding a contributor, in which case it will have no
            # corresponding name and can be ignored.
            if name:
                if role not in list(Contributor.MARC_ROLE_CODES.keys()):
                    self._db.rollback()
                    return UNKNOWN_ROLE.detailed(
                        _(
                            "Role %(role)s is not one of the known contributor roles.",
                            role=role,
                        )
                    )
                contributor = staff_edition.add_contributor(name=name, roles=[role])
                contributor.display_name = name
                changed = True

        new_series = flask.request.form.get("series")
        if work.series != new_series:
            if work.series and not new_series:
                new_series = NO_VALUE
            staff_edition.series = str(new_series)
            changed = True

        series_position_str = flask.request.form.get("series_position")
        new_series_position: int | None
        if series_position_str is not None and series_position_str != "":
            try:
                new_series_position = int(series_position_str)
            except ValueError:
                self._db.rollback()
                return INVALID_SERIES_POSITION
        else:
            new_series_position = None
        if work.series_position != new_series_position:
            if work.series_position and new_series_position == None:
                new_series_position = NO_NUMBER
            staff_edition.series_position = new_series_position
            changed = True

        new_medium = flask.request.form.get("medium")
        if new_medium:
            if new_medium not in list(Edition.medium_to_additional_type.keys()):
                self._db.rollback()
                return UNKNOWN_MEDIUM.detailed(
                    _(
                        "Medium %(medium)s is not one of the known media.",
                        medium=new_medium,
                    )
                )
            staff_edition.medium = new_medium
            changed = True

        language_str = flask.request.form.get("language")
        if language_str is not None and language_str != "":
            new_language = LanguageCodes.string_to_alpha_3(language_str)
            if not new_language:
                self._db.rollback()
                return UNKNOWN_LANGUAGE
        else:
            new_language = None
        if new_language != staff_edition.language:
            staff_edition.language = new_language
            changed = True

        new_publisher = flask.request.form.get("publisher")
        if new_publisher != staff_edition.publisher:
            if staff_edition.publisher and not new_publisher:
                new_publisher = NO_VALUE
            staff_edition.publisher = str(new_publisher)
            changed = True

        new_imprint = flask.request.form.get("imprint")
        if new_imprint != staff_edition.imprint:
            if staff_edition.imprint and not new_imprint:
                new_imprint = NO_VALUE
            staff_edition.imprint = str(new_imprint)
            changed = True

        issued_str = flask.request.form.get("issued")
        if issued_str is not None and issued_str != "":
            try:
                new_issued = strptime_utc(issued_str, "%Y-%m-%d")
            except ValueError:
                self._db.rollback()
                return INVALID_DATE_FORMAT
        else:
            new_issued = None
        if new_issued != staff_edition.issued:
            staff_edition.issued = new_issued
            changed = True

        # TODO: This lets library staff add a 1-5 rating, which is used in the
        # quality calculation. However, this doesn't work well if there are any
        # other measurements that contribute to the quality. The form will show
        # the calculated quality rather than the staff rating, which will be
        # confusing. It might also be useful to make it more clear how this
        # relates to the quality threshold in the library settings.
        changed_rating = False
        rating_str = flask.request.form.get("rating")
        new_rating_value: float | None = None
        if rating_str is not None and rating_str != "":
            try:
                new_rating_value = float(rating_str)
            except ValueError:
                self._db.rollback()
                return INVALID_RATING
            scale = Measurement.RATING_SCALES[DataSource.LIBRARY_STAFF]
            if new_rating_value < scale[0] or new_rating_value > scale[1]:
                self._db.rollback()
                return INVALID_RATING.detailed(
                    _(
                        "The rating must be a number between %(low)s and %(high)s.",
                        low=scale[0],
                        high=scale[1],
                    )
                )
            assert staff_data_source is not None
            if (new_rating_value - scale[0]) / (scale[1] - scale[0]) != work.quality:
                primary_identifier.add_measurement(
                    staff_data_source,
                    Measurement.RATING,
                    new_rating_value,
                    weight=WorkController.STAFF_WEIGHT,
                )
                changed = True
                changed_rating = True

        changed_summary = False
        new_summary = flask.request.form.get("summary") or ""
        if new_summary != work.summary_text:
            old_summary = None
            if work.summary and work.summary.data_source == staff_data_source:
                old_summary = work.summary

            work.presentation_edition.primary_identifier.add_link(
                Hyperlink.DESCRIPTION, None, staff_data_source, content=new_summary
            )

            # Delete previous staff summary
            if old_summary:
                for link in old_summary.links:
                    self._db.delete(link)
                self._db.delete(old_summary)

            changed = True
            changed_summary = True

        if changed:
            # Even if the presentation doesn't visibly change, we want
            # to regenerate the OPDS entries and update the search
            # index for the work, because that might be the 'real'
            # problem the user is trying to fix.
            policy = PresentationCalculationPolicy(
                classify=True,
                update_search_index=True,
                calculate_quality=changed_rating,
                choose_summary=changed_summary,
            )
            work.calculate_presentation(policy=policy)

        return Response("", 200)

    def suppress(
        self, identifier_type: str, identifier: str
    ) -> Response | ProblemDetail:
        """Suppress a book at the level of a library."""

        library: Library | None = get_request_library(default=None)
        if library is None:
            raise ProblemDetailException(LIBRARY_NOT_FOUND)

        self.require_library_manager(library)

        work = self.load_work(
            library=library, identifier_type=identifier_type, identifier=identifier
        )

        if isinstance(work, ProblemDetail):
            # Something went wrong.
            return work

        if library in work.suppressed_for:
            # If the library is already suppressed, we don't need to do anything.
            message = f"Already suppressed {identifier_type}/{identifier} (work id: {work.id}) for library {library.short_name}."
        else:
            # Otherwise, add the library to the suppressed list.
            work.suppressed_for.append(library)
            message = f"Suppressed {identifier_type}/{identifier} (work id: {work.id}) for library {library.short_name}."

        self.log.info(message)
        return Response(
            json.dumps({"message": message}), 200, mimetype="application/json"
        )

    def unsuppress(
        self, identifier_type: str, identifier: str
    ) -> Response | ProblemDetail:
        """Remove a book suppression from a book at the level of a library"""

        library: Library | None = get_request_library(default=None)
        if library is None:
            raise ProblemDetailException(LIBRARY_NOT_FOUND)

        self.require_library_manager(library)

        work = self.load_work(
            library=library, identifier_type=identifier_type, identifier=identifier
        )

        if isinstance(work, ProblemDetail):
            # Something went wrong.
            return work

        if library not in work.suppressed_for:
            # If the library is not suppressed, we don't need to do anything.
            message = f"Already unsuppressed {identifier_type}/{identifier} (work id: {work.id}) for library {library.short_name}."
        else:
            # Otherwise, remove the library from the suppressed list.
            work.suppressed_for.remove(library)
            message = f"Unsuppressed {identifier_type}/{identifier} (work id: {work.id}) for library {library.short_name}."

        self.log.info(message)
        return Response(
            json.dumps({"message": message}), 200, mimetype="application/json"
        )

    def refresh_metadata(
        self, identifier_type: str, identifier: str, provider: Any | None = None
    ) -> Response | ProblemDetail:
        """Refresh the metadata for a book from the content server"""
        library = get_request_library()
        self.require_librarian(library)

        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        if provider is None:
            return METADATA_REFRESH_FAILURE

        assert work.presentation_edition is not None
        primary_identifier = work.presentation_edition.primary_identifier
        try:
            record = provider.ensure_coverage(primary_identifier, force=True)
        except Exception:
            # The coverage provider may raise an HTTPIntegrationException.
            return REMOTE_INTEGRATION_FAILED

        if record.exception:
            # There was a coverage failure.
            if str(record.exception).startswith("201") or str(
                record.exception
            ).startswith("202"):
                # A 201/202 error means it's never looked up this work before
                # so it's started the resolution process or looking for sources.
                return METADATA_REFRESH_PENDING
            # Otherwise, it just doesn't know anything.
            return METADATA_REFRESH_FAILURE

        return Response("", 200)

    def classifications(
        self, identifier_type: str, identifier: str
    ) -> dict[str, Any] | ProblemDetail:
        """Return list of this work's classifications."""
        library = get_request_library()
        self.require_librarian(library)

        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        assert work.presentation_edition is not None
        identifier_id = work.presentation_edition.primary_identifier.id
        results = (
            self._db.query(Classification)
            .join(Subject)
            .join(DataSource)
            .filter(Classification.identifier_id == identifier_id)
            .order_by(Classification.weight.desc())
            .all()
        )

        data = []
        for result in results:
            data.append(
                dict(
                    {
                        "type": result.subject.type,
                        "name": result.subject.identifier,
                        "source": result.data_source.name,
                        "weight": result.weight,
                    }
                )
            )

        return dict(
            {
                "book": {"identifier_type": identifier_type, "identifier": identifier},
                "classifications": data,
            }
        )

    def edit_classifications(
        self, identifier_type: str, identifier: str
    ) -> Response | ProblemDetail:
        """Edit a work's audience, target age, fiction status, and genres."""
        library = get_request_library()
        self.require_librarian(library)

        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        if staff_data_source is None:
            self._db.rollback()
            return INVALID_INPUT
        assert work.presentation_edition is not None

        # Previous staff classifications
        primary_identifier = work.presentation_edition.primary_identifier
        old_classifications = (
            self._db.query(Classification)
            .join(Subject)
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source,
            )
        )
        old_genre_classifications = old_classifications.filter(Subject.genre_id != None)
        old_staff_genres = [
            c.subject.genre.name for c in old_genre_classifications if c.subject.genre
        ]
        old_computed_genres = [work_genre.genre.name for work_genre in work.work_genres]

        # New genres should be compared to previously computed genres
        new_genres = flask.request.form.getlist("genres")
        genres_changed = sorted(new_genres) != sorted(old_computed_genres)

        # Update audience
        new_audience = flask.request.form.get("audience")
        if new_audience != work.audience:
            # Delete all previous staff audience classifications
            for c in old_classifications:
                if c.subject.type == Subject.FREEFORM_AUDIENCE:
                    self._db.delete(c)

            # Create a new classification with a high weight
            primary_identifier.classify(
                data_source=staff_data_source,
                subject_type=Subject.FREEFORM_AUDIENCE,
                subject_identifier=new_audience,
                weight=WorkController.STAFF_WEIGHT,
            )

        # Update target age if present
        target_age_min_str = flask.request.form.get("target_age_min")
        new_target_age_min = int(target_age_min_str) if target_age_min_str else None
        target_age_max_str = flask.request.form.get("target_age_max")
        new_target_age_max = int(target_age_max_str) if target_age_max_str else None
        if (
            new_target_age_max is not None
            and new_target_age_min is not None
            and new_target_age_max < new_target_age_min
        ):
            return INVALID_EDIT.detailed(
                _("Minimum target age must be less than maximum target age.")
            )

        if work.target_age:
            old_target_age_min = work.target_age.lower
            old_target_age_max = work.target_age.upper
        else:
            old_target_age_min = None
            old_target_age_max = None
        if (
            new_target_age_min != old_target_age_min
            or new_target_age_max != old_target_age_max
        ):
            # Delete all previous staff target age classifications
            for c in old_classifications:
                if c.subject.type == Subject.AGE_RANGE:
                    self._db.delete(c)

            # Create a new classification with a high weight - higher than audience
            if new_target_age_min and new_target_age_max:
                age_range_identifier = "{}-{}".format(
                    new_target_age_min,
                    new_target_age_max,
                )
                primary_identifier.classify(
                    data_source=staff_data_source,
                    subject_type=Subject.AGE_RANGE,
                    subject_identifier=age_range_identifier,
                    weight=WorkController.STAFF_WEIGHT * 100,
                )

        # Update fiction status
        # If fiction status hasn't changed but genres have changed,
        # we still want to ensure that there's a staff classification
        new_fiction = True if flask.request.form.get("fiction") == "fiction" else False
        if new_fiction != work.fiction or genres_changed:
            # Delete previous staff fiction classifications
            for c in old_classifications:
                if c.subject.type == Subject.SIMPLIFIED_FICTION_STATUS:
                    self._db.delete(c)

            # Create a new classification with a high weight (higher than genre)
            fiction_term = "Fiction" if new_fiction else "Nonfiction"
            classification = primary_identifier.classify(
                data_source=staff_data_source,
                subject_type=Subject.SIMPLIFIED_FICTION_STATUS,
                subject_identifier=fiction_term,
                weight=WorkController.STAFF_WEIGHT,
            )
            classification.subject.fiction = new_fiction

        # Update genres
        # make sure all new genres are legit
        for name in new_genres:
            genre_obj, is_new = Genre.lookup(self._db, name)
            if not isinstance(genre_obj, Genre):
                return GENRE_NOT_FOUND
            if (
                genres[name].is_fiction is not None
                and genres[name].is_fiction != new_fiction
            ):
                return INCOMPATIBLE_GENRE
            if name == "Erotica" and new_audience != "Adults Only":
                return EROTICA_FOR_ADULTS_ONLY

        if genres_changed:
            # delete existing staff classifications for genres that aren't being kept
            for c in old_genre_classifications:
                if c.subject.genre.name not in new_genres:
                    self._db.delete(c)

            # add new staff classifications for new genres
            for genre in new_genres:
                if genre not in old_staff_genres:
                    classification = primary_identifier.classify(
                        data_source=staff_data_source,
                        subject_type=Subject.SIMPLIFIED_GENRE,
                        subject_identifier=genre,
                        weight=WorkController.STAFF_WEIGHT,
                    )

            # add NONE genre classification if we aren't keeping any genres
            if len(new_genres) == 0:
                primary_identifier.classify(
                    data_source=staff_data_source,
                    subject_type=Subject.SIMPLIFIED_GENRE,
                    subject_identifier=SimplifiedGenreClassifier.NONE,
                    weight=WorkController.STAFF_WEIGHT,
                )
            else:
                # otherwise delete existing NONE genre classification
                none_classifications = (
                    self._db.query(Classification)
                    .join(Subject)
                    .filter(
                        Classification.identifier == primary_identifier,
                        Subject.identifier == SimplifiedGenreClassifier.NONE,
                    )
                    .all()
                )
                for c in none_classifications:
                    self._db.delete(c)

        # Update presentation
        policy = PresentationCalculationPolicy(
            classify=True,
            update_search_index=True,
        )
        work.calculate_presentation(policy=policy)

        return Response("", 200)

    @staticmethod
    def _existing_custom_lists(library: Library, work: Work) -> list[CustomList]:
        return [
            entry.customlist
            for entry in work.custom_list_entries
            if entry.customlist and entry.customlist.library == library
        ]

    def _custom_lists_get(self, library: Library, work: Work) -> dict[str, Any]:
        lists = [
            CustomListResponse(id=cl.id, name=cl.name).api_dict()
            for cl in self._existing_custom_lists(library, work)
        ]
        return dict(custom_lists=lists)

    def _custom_lists_post(self, library: Library, work: Work) -> Response:
        ta = TypeAdapter(list[CustomListResponse])
        try:
            lists = ta.validate_json(flask.request.form.get("lists", "[]", str))
        except ValidationError as ex:
            self.log.debug("Invalid custom list data: %s", ex)
            raise ProblemDetailException(
                INVALID_INPUT.detailed("Invalid form data", debug_message=str(ex))
            )

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        if staff_data_source is None:
            self._db.rollback()
            raise ProblemDetailException(INVALID_INPUT)
        affected_lanes = set()

        # Remove entries for lists that were not in the submitted form.
        submitted_ids = {l.id for l in lists}
        for custom_list in self._existing_custom_lists(library, work):
            if custom_list.id not in submitted_ids:
                custom_list.remove_entry(work)
                for lane in Lane.affected_by_customlist(custom_list):
                    affected_lanes.add(lane)

        # Add entries for any new lists.
        for list_response in lists:
            if list_response.id is not None:
                custom_list_or_none = get_one(
                    self._db,
                    CustomList,
                    id=list_response.id,
                    name=list_response.name,
                    library=library,
                    data_source=staff_data_source,
                )
                if not custom_list_or_none:
                    self._db.rollback()
                    raise ProblemDetailException(
                        MISSING_CUSTOM_LIST.detailed(
                            _(
                                'Could not find list "%(list_name)s"',
                                list_name=list_response.name,
                            )
                        )
                    )
                custom_list = custom_list_or_none
            else:
                custom_list, __ = create(
                    self._db,
                    CustomList,
                    name=list_response.name,
                    data_source=staff_data_source,
                    library=library,
                )
                custom_list.created = utc_now()
            entry, was_new = custom_list.add_entry(work, featured=True)
            if was_new:
                for lane in Lane.affected_by_customlist(custom_list):
                    affected_lanes.add(lane)

        # If any list changes affected lanes, update their sizes.
        # NOTE: This may not make a difference until the
        # works are actually re-indexed.
        for lane in affected_lanes:
            lane.update_size(self._db, search_engine=self.search_engine)

        return Response(str(_("Success")), 200)

    def custom_lists(
        self, identifier_type: str, identifier: str
    ) -> Mapping[str, Any] | Response:
        library = get_request_library()
        self.require_librarian(library)
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            raise ProblemDetailException(work)

        if flask.request.method == "GET":
            return self._custom_lists_get(library, work)

        elif flask.request.method == "POST":
            return self._custom_lists_post(library, work)

        else:
            raise RuntimeError("Unsupported method")
