import logging
from typing import Dict

import boto3
import flask
from sqlalchemy import select

from api.admin.model.quicksight import (
    QuicksightDashboardNamesResponse,
    QuicksightGenerateUrlRequest,
    QuicksightGenerateUrlResponse,
)
from api.controller import CirculationManagerController
from api.problem_details import NOT_FOUND_ON_REMOTE
from core.config import Configuration
from core.model.admin import Admin
from core.model.library import Library
from core.problem_details import INTERNAL_SERVER_ERROR, INVALID_INPUT
from core.util.problem_detail import ProblemError


class QuickSightController(CirculationManagerController):
    def generate_quicksight_url(self, dashboard_name) -> Dict:
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

        if request_data.library_ids:
            allowed_library_ids = list(
                set(request_data.library_ids).intersection(
                    {l.id for l in allowed_libraries}
                )
            )
        else:
            allowed_library_ids = [l.id for l in allowed_libraries]

        if not allowed_library_ids:
            raise ProblemError(
                NOT_FOUND_ON_REMOTE.detailed(
                    "No library was found for this Admin that matched the request."
                )
            )

        libraries = self._db.execute(
            select(Library.name)
            .where(Library.id.in_(allowed_library_ids))
            .order_by(Library.name)
        ).all()

        try:
            delimiter = "|"
            client = boto3.client("quicksight", region_name=region)
            response = client.generate_embed_url_for_anonymous_user(
                AwsAccountId=aws_account_id,
                Namespace="default",  # Default namespace only
                AuthorizedResourceArns=authorized_arns,
                ExperienceConfiguration={
                    "Dashboard": {"InitialDashboardId": dashboard_id}
                },
                SessionTags=[
                    dict(
                        Key="library_name",
                        Value=delimiter.join([l.name for l in libraries]),
                    )
                ],
            )
        except Exception as ex:
            log.error(f"Error while fetching the Quisksight Embed url: {ex}")
            raise ProblemError(
                INTERNAL_SERVER_ERROR.detailed(
                    "Error while fetching the Quisksight Embed url."
                )
            )

        embed_url = response.get("EmbedUrl")
        if response.get("Status") // 100 != 2 or embed_url is None:
            log.error(f"QuiskSight Embed url error response {response}")
            raise ProblemError(
                INTERNAL_SERVER_ERROR.detailed(
                    "Error while fetching the Quisksight Embed url."
                )
            )

        return QuicksightGenerateUrlResponse(embed_url=embed_url).api_dict()

    def get_dashboard_names(self):
        """Get the named dashboard IDs defined in the configuration"""
        config = Configuration.quicksight_authorized_arns()
        return QuicksightDashboardNamesResponse(names=list(config.keys())).api_dict()
