import logging
from typing import Any

import boto3
import flask
from sqlalchemy import select

from api.admin.model.quicksight import (
    QuicksightDashboardNamesResponse,
    QuicksightGenerateUrlRequest,
    QuicksightGenerateUrlResponse,
)
from api.controller.circulation_manager import CirculationManagerController
from api.problem_details import NOT_FOUND_ON_REMOTE
from core.config import Configuration
from core.model.admin import Admin
from core.model.library import Library
from core.problem_details import INTERNAL_SERVER_ERROR, INVALID_INPUT
from core.util.problem_detail import ProblemError


class QuickSightController(CirculationManagerController):
    def generate_quicksight_url(self, dashboard_name) -> dict:
        log = logging.getLogger(self.__class__.__name__)
        admin: Admin = getattr(flask.request, "admin")
        request_data = QuicksightGenerateUrlRequest(**flask.request.args)

        all_authorized_arns = Configuration.quicksight_authorized_arns()
        if not all_authorized_arns:
            log.error("No Quicksight ARNs were configured for this server.")
            raise ProblemError(
                INTERNAL_SERVER_ERROR.detailed(
                    "Quicksight has not been configured for this server."
                )
            )

        authorized_arns = all_authorized_arns.get(dashboard_name)
        if not authorized_arns:
            raise ProblemError(
                INVALID_INPUT.detailed(
                    "The requested Dashboard ARN is not recognized by this server."
                )
            )

        # The first dashboard id is the primary ARN
        dashboard_arn = authorized_arns[0]
        # format aws:arn:quicksight:<region>:<account id>:<dashboard>
        arn_parts = dashboard_arn.split(":")
        # Pull the region and account id from the ARN
        aws_account_id = arn_parts[4]
        region = arn_parts[3]
        dashboard_id = arn_parts[5].split("/", 1)[1]  # drop the "dashboard/" part

        allowed_libraries = []
        for library in self._db.query(Library).all():
            if admin.is_librarian(library):
                allowed_libraries.append(library)

        if request_data.library_uuids:
            allowed_library_uuids = list(
                set(map(str, request_data.library_uuids)).intersection(
                    {l.uuid for l in allowed_libraries}
                )
            )
        else:
            allowed_library_uuids = [l.uuid for l in allowed_libraries]

        if not allowed_library_uuids:
            raise ProblemError(
                NOT_FOUND_ON_REMOTE.detailed(
                    "No library was found for this Admin that matched the request."
                )
            )

        libraries = self._db.execute(
            select(Library.short_name)
            .where(Library.uuid.in_(allowed_library_uuids))
            .order_by(Library.name)
        ).all()

        try:
            short_names = [x[0] for x in libraries]
            session_tags = self._build_session_tags_array(short_names)

            client = boto3.client("quicksight", region_name=region)
            response = client.generate_embed_url_for_anonymous_user(
                AwsAccountId=aws_account_id,
                Namespace="default",  # Default namespace only
                AuthorizedResourceArns=authorized_arns,
                ExperienceConfiguration={
                    "Dashboard": {"InitialDashboardId": dashboard_id}
                },
                SessionTags=session_tags,
            )
        except Exception as ex:
            log.error(f"Error while fetching the Quicksight Embed url: {ex}")
            raise ProblemError(
                INTERNAL_SERVER_ERROR.detailed(
                    "Error while fetching the Quicksight Embed url."
                )
            )

        embed_url = response.get("EmbedUrl")
        if response.get("Status") // 100 != 2 or embed_url is None:
            log.error(f"Quicksight Embed url error response {response}")
            raise ProblemError(
                INTERNAL_SERVER_ERROR.detailed(
                    "Error while fetching the Quicksight Embed url."
                )
            )

        return QuicksightGenerateUrlResponse(embed_url=embed_url).api_dict()

    def _build_session_tags_array(self, short_names: list[str]) -> list[dict]:
        def append_to_session_tags():
            session_tags.append(
                dict(
                    Key=f"library_short_name_{tag_index}",
                    Value=delimiter.join(tag_values),
                )
            )

        delimiter = "|"
        max_chars_per_tag = 256

        session_tags: list[dict[Any, str]] = []
        per_tag_character_count = 0
        tag_index = 0
        tag_values = []
        for short_name in short_names:
            chars_to_be_added = len(short_name) + 1
            if chars_to_be_added + per_tag_character_count <= max_chars_per_tag:
                tag_values.append(short_name)
                per_tag_character_count += chars_to_be_added
            else:
                append_to_session_tags()
                per_tag_character_count = chars_to_be_added
                tag_values = [short_name]
                tag_index += 1

        append_to_session_tags()

        return session_tags

    def get_dashboard_names(self):
        """Get the named dashboard IDs defined in the configuration"""
        config = Configuration.quicksight_authorized_arns()
        return QuicksightDashboardNamesResponse(names=list(config.keys())).api_dict()
