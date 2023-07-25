from __future__ import annotations

from typing import Optional, Union

import flask
from flask import Request, Response, redirect, url_for
from flask_babel import lazy_gettext as _
from werkzeug import Response as WerkzeugResponse

from api.admin.config import Configuration as AdminClientConfig
from api.admin.controller.base import AdminController
from api.admin.controller.sign_in import SignInController
from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.admin.problem_details import (
    ADMIN_AUTH_MECHANISM_NOT_CONFIGURED,
    ADMIN_AUTH_NOT_CONFIGURED,
    INVALID_ADMIN_CREDENTIALS,
)
from api.admin.template_styles import (
    body_style,
    error_style,
    hr_style,
    small_link_style,
)
from api.admin.templates import response_template_with_message_and_redirect_button
from core.model import Admin, get_one
from core.util.problem_detail import ProblemDetail


class ResetPasswordController(AdminController):
    FORGOT_PASSWORD_TEMPLATE = SignInController.SIGN_IN_TEMPLATE
    RESET_PASSWORD_TEMPLATE = SignInController.SIGN_IN_TEMPLATE

    HEAD_TEMPLATE = SignInController.HEAD_TEMPLATE

    RESPONSE_TEMPLATE_WITH_MESSAGE = (
        response_template_with_message_and_redirect_button.format(
            head_html=HEAD_TEMPLATE, hr=hr_style, link=small_link_style
        )
    )

    def forgot_password(self) -> Union[ProblemDetail, WerkzeugResponse]:
        """Shows forgot password page or starts off forgot password workflow"""

        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        auth = self.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin = self.authenticated_admin_from_request()

        admin_view_redirect = redirect(url_for("admin_view"))

        if isinstance(admin, Admin):
            return admin_view_redirect

        if flask.request.method == "GET":
            auth_provider_html = auth.forgot_password_template(admin_view_redirect)

            html = self.FORGOT_PASSWORD_TEMPLATE % dict(
                auth_provider_html=auth_provider_html,
                logo_url=AdminClientConfig.lookup_asset_url(key="admin_logo"),
            )
            headers = dict()
            headers["Content-Type"] = "text/html"

            return Response(html, 200, headers)

        admin = self._extract_admin_from_request(flask.request)

        if not admin:
            return self._response_with_message_and_redirect_button(
                INVALID_ADMIN_CREDENTIALS.detail,
                url_for("admin_forgot_password"),
                "Try again",
                is_error=True,
                status_code=INVALID_ADMIN_CREDENTIALS.status_code,
            )

        reset_password_url = self._generate_reset_password_url(admin, auth)

        auth.send_reset_password_email(admin, reset_password_url)

        return self._response_with_message_and_redirect_button(
            "Email successfully sent! Please check your inbox.",
            url_for("admin_sign_in"),
            "Sign in",
        )

    def _extract_admin_from_request(self, request: Request) -> Optional[Admin]:
        email = request.form.get("email")

        admin = get_one(self._db, Admin, email=email)

        return admin

    def _generate_reset_password_url(
        self, admin: Admin, auth: PasswordAdminAuthenticationProvider
    ) -> str:
        reset_password_token = auth.generate_reset_password_token(admin, self._db)

        reset_password_url = url_for(
            "admin_reset_password",
            reset_password_token=reset_password_token,
            admin_id=admin.id,
            _external=True,
        )

        return reset_password_url

    def reset_password(
        self, reset_password_token: str, admin_id: int
    ) -> Optional[WerkzeugResponse]:
        """Shows reset password page or process the reset password request"""
        auth = self.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return self._response_with_message_and_redirect_button(
                ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.detail,
                url_for("admin_sign_in"),
                "Sign in",
                is_error=True,
                status_code=ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.status_code,
            )

        logged_in_admin = self.authenticated_admin_from_request()

        admin_view_redirect = redirect(url_for("admin_view"))

        # If the admin is logged in we redirect it since in that case the logged in change password option can be used
        if isinstance(logged_in_admin, Admin):
            return admin_view_redirect

        admin_from_token = auth.validate_token_and_extract_admin(
            reset_password_token, admin_id, self._db
        )

        if isinstance(admin_from_token, ProblemDetail):
            return self._response_with_message_and_redirect_button(
                admin_from_token.detail,
                url_for("admin_forgot_password"),
                "Try again",
                is_error=True,
                status_code=admin_from_token.status_code,
            )

        if flask.request.method == "GET":
            auth_provider_html = auth.reset_password_template(
                reset_password_token, admin_id, admin_view_redirect
            )

            html = self.RESET_PASSWORD_TEMPLATE % dict(
                auth_provider_html=auth_provider_html,
                logo_url=AdminClientConfig.lookup_asset_url(key="admin_logo"),
            )
            headers = dict()
            headers["Content-Type"] = "text/html"

            return Response(html, 200, headers)

        if flask.request.method == "POST":
            new_password = flask.request.form.get("password")
            confirm_password = flask.request.form.get("confirm_password")

            if new_password and confirm_password and new_password == confirm_password:
                admin_from_token.password = new_password

            else:
                problem_detail = INVALID_ADMIN_CREDENTIALS.detailed(
                    _("Passwords do not match.")
                )

                return self._response_with_message_and_redirect_button(
                    problem_detail.detail,
                    url_for(
                        "admin_reset_password",
                        reset_password_token=reset_password_token,
                        admin_id=admin_id,
                    ),
                    "Try again",
                    is_error=True,
                    status_code=problem_detail.status_code,
                )

            return self._response_with_message_and_redirect_button(
                "Password successfully changed!",
                url_for("admin_sign_in"),
                "Sign in",
            )

        return None

    def _response_with_message_and_redirect_button(
        self,
        message: Optional[str],
        redirect_button_link: str,
        redirect_button_text: str,
        is_error: bool = False,
        status_code: Optional[int] = 200,
    ) -> Response:
        style = error_style if is_error else body_style

        html = self.RESPONSE_TEMPLATE_WITH_MESSAGE % dict(
            body_style=style,
            message=message,
            redirect_link=redirect_button_link,
            button_text=redirect_button_text,
        )

        return Response(html, status_code)
