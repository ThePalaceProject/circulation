from __future__ import annotations

import logging
from urllib.parse import urlsplit

import flask
from flask import Response, redirect, url_for
from flask_babel import lazy_gettext as _
from werkzeug import Response as WerkzeugResponse

from api.admin.config import Configuration as AdminClientConfig
from api.admin.controller.base import AdminController
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
    logo_style,
    section_style,
    small_link_style,
)
from core.util.problem_detail import ProblemDetail


class SignInController(AdminController):
    HEAD_TEMPLATE = """<head>
<meta charset="utf8">
<title>{app_name}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;700&display=swap');
</style>
</head>
""".format(
        app_name=AdminClientConfig.APP_NAME
    )

    ERROR_RESPONSE_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
{head_html}
<body style="{error}">
<p><strong>%(status_code)d ERROR:</strong> %(message)s</p>
<hr style="{hr}">
<a href="/admin/sign_in" style="{link}">Try again</a>
</body>
</html>""".format(
        head_html=HEAD_TEMPLATE, error=error_style, hr=hr_style, link=small_link_style
    )

    SIGN_IN_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
{head_html}
<body style="{body}">
<img src="%(logo_url)s" alt="{app_name}" style="{logo}">
%(auth_provider_html)s
</body>
</html>""".format(
        head_html=HEAD_TEMPLATE,
        body=body_style,
        app_name=AdminClientConfig.APP_NAME,
        logo=logo_style,
    )

    def sign_in(self):
        """Redirects admin if they're signed in, or shows the sign in page."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        admin = self.authenticated_admin_from_request()

        if isinstance(admin, ProblemDetail):
            redirect_url = flask.request.args.get("redirect")
            auth_provider_html = [
                auth.sign_in_template(redirect_url)
                for auth in self.admin_auth_providers
            ]
            auth_provider_html = """
                <section style="{section}">
                <hr style="{hr}">or<hr style="{hr}">
                </section>
            """.format(
                section=section_style, hr=hr_style
            ).join(
                auth_provider_html
            )

            html = self.SIGN_IN_TEMPLATE % dict(
                auth_provider_html=auth_provider_html,
                logo_url=AdminClientConfig.lookup_asset_url(key="admin_logo"),
            )
            headers = dict()
            headers["Content-Type"] = "text/html"
            return Response(html, 200, headers)
        elif admin:
            return SanitizedRedirections.redirect(flask.request.args.get("redirect"))

    def password_sign_in(self):
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        auth = self.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin_details, redirect_url = auth.sign_in(self._db, flask.request.form)
        if isinstance(admin_details, ProblemDetail):
            return self.error_response(INVALID_ADMIN_CREDENTIALS)

        admin = self.authenticated_admin(admin_details)
        return SanitizedRedirections.redirect(redirect_url)

    def change_password(self):
        admin = flask.request.admin
        new_password = flask.request.form.get("password")
        if new_password:
            admin.password = new_password
        return Response(_("Success"), 200)

    def sign_out(self):
        # Clear out the admin's flask session.
        flask.session.pop("admin_email", None)
        flask.session.pop("auth_type", None)

        redirect_url = url_for(
            "admin_sign_in",
            redirect=url_for("admin_view", _external=True),
            _external=True,
        )
        return SanitizedRedirections.redirect(redirect_url)

    def error_response(self, problem_detail):
        """Returns a problem detail as an HTML response"""
        html = self.ERROR_RESPONSE_TEMPLATE % dict(
            status_code=problem_detail.status_code, message=problem_detail.detail
        )
        return Response(html, problem_detail.status_code)


class SanitizedRedirections:
    """Functions to sanitize redirects."""

    @staticmethod
    def _check_redirect(target: str) -> tuple[bool, str]:
        """Check that a redirect is allowed.
        Because the URL redirect is assumed to be untrusted user input,
        we extract the URL path and forbid redirecting to external
        hosts.
        """
        redirect_url = urlsplit(target)

        # If the redirect isn't asking for a particular host, then it's safe.
        if redirect_url.netloc in (None, ""):
            return True, ""

        # Otherwise, if the redirect is asking for a different host, it's unsafe.
        if redirect_url.netloc != flask.request.host:
            logging.warning(f"Redirecting to {redirect_url.netloc} is not permitted")
            return False, _("Redirecting to an external domain is not allowed.")

        return True, ""

    @staticmethod
    def redirect(target: str) -> WerkzeugResponse:
        """Check that a redirect is allowed before performing it."""
        ok, message = SanitizedRedirections._check_redirect(target)
        if ok:
            return redirect(target, Response=Response)
        else:
            return Response(message, 400)
