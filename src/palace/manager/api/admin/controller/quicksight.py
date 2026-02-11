import boto3
import flask
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.api.admin.model.quicksight import (
    QuicksightDashboardNamesResponse,
    QuicksightGenerateUrlRequest,
    QuicksightGenerateUrlResponse,
)
from palace.manager.api.admin.util.flask import get_request_admin
from palace.manager.api.problem_details import NOT_FOUND_ON_REMOTE
from palace.manager.core.problem_details import INTERNAL_SERVER_ERROR, INVALID_INPUT
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetailException


class QuickSightController(LoggerMixin):
    def __init__(
        self, db: Session, authorized_arns: dict[str, list[str]] | None
    ) -> None:
        self._db = db
        self._authorized_arns = authorized_arns

    @property
    def authorized_arns(self) -> dict[str, list[str]]:
        if self._authorized_arns is None:
            self.log.error("No Quicksight ARNs were configured for this server.")
            raise ProblemDetailException(
                INTERNAL_SERVER_ERROR.detailed(
                    "Quicksight has not been configured for this server."
                )
            )
        return self._authorized_arns

    def dashboard_authorized_arns(self, dashboard_name: str) -> list[str]:
        arns = self.authorized_arns.get(dashboard_name)
        if not arns:
            raise ProblemDetailException(
                INVALID_INPUT.detailed(
                    "The requested Dashboard ARN is not recognized by this server."
                )
            )
        return arns

    def generate_quicksight_url(self, dashboard_name: str) -> dict[str, str]:
        admin: Admin = get_request_admin()
        request_data = QuicksightGenerateUrlRequest(**flask.request.args)

        authorized_arns = self.dashboard_authorized_arns(dashboard_name)

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
            raise ProblemDetailException(
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
            short_names = [x.short_name for x in libraries]
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
            self.log.exception(f"Error while fetching the Quicksight Embed url: {ex}")
            raise ProblemDetailException(
                INTERNAL_SERVER_ERROR.detailed(
                    "Error while fetching the Quicksight Embed url."
                )
            )

        embed_url = response.get("EmbedUrl")
        status = response.get("Status")
        if status is None or embed_url is None or status // 100 != 2:
            self.log.error(f"Quicksight Embed url error response {response}")
            raise ProblemDetailException(
                INTERNAL_SERVER_ERROR.detailed(
                    "Error while fetching the Quicksight Embed url."
                )
            )

        return QuicksightGenerateUrlResponse(embed_url=embed_url).api_dict()

    def _build_session_tags_array(self, short_names: list[str]) -> list[dict[str, str]]:
        delimiter = "|"  # specified by AWS's session tag limit
        max_chars_per_tag = 256
        session_tags: list[str] = []
        session_tag = ""
        for short_name in short_names:
            if len(session_tag + delimiter + short_name) > max_chars_per_tag:
                session_tags.append(session_tag)
                session_tag = ""
            if session_tag:
                session_tag += delimiter + short_name
            else:
                session_tag = short_name
        if session_tag:
            session_tags.append(session_tag)

        return [
            {
                "Key": f"library_short_name_{tag_index}",
                "Value": tag_value,
            }
            for tag_index, tag_value in enumerate(session_tags)
        ]

    def get_dashboard_names(self) -> dict[str, list[str]]:
        """Get the named dashboard IDs defined in the configuration"""
        return QuicksightDashboardNamesResponse(
            names=list(self.authorized_arns.keys())
        ).api_dict()
