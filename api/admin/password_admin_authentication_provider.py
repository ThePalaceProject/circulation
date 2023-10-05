from typing import Union

from flask import render_template_string, url_for
from sqlalchemy.orm.session import Session

from api.admin.admin_authentication_provider import AdminAuthenticationProvider
from api.admin.config import Configuration as AdminClientConfig
from api.admin.problem_details import INVALID_ADMIN_CREDENTIALS
from api.admin.template_styles import button_style, input_style, label_style
from api.admin.templates import (
    forgot_password_template,
    reset_password_email_html,
    reset_password_email_text,
    reset_password_template,
    sign_in_template,
)
from api.config import Configuration
from core.model import Admin, ConfigurationSetting
from core.util.email import EmailManager
from core.util.problem_detail import ProblemDetail


class PasswordAdminAuthenticationProvider(AdminAuthenticationProvider):
    NAME = "Password Auth"

    SIGN_IN_TEMPLATE = sign_in_template.format(
        label=label_style, input=input_style, button=button_style
    )

    FORGOT_PASSWORD_TEMPLATE = forgot_password_template.format(
        label=label_style, input=input_style, button=button_style
    )

    RESET_PASSWORD_TEMPLATE = reset_password_template.format(
        label=label_style, input=input_style, button=button_style
    )

    def sign_in_template(self, redirect):
        password_sign_in_url = url_for("password_auth")
        forgot_password_url = url_for("admin_forgot_password")
        return self.SIGN_IN_TEMPLATE % dict(
            redirect=redirect,
            password_sign_in_url=password_sign_in_url,
            forgot_password_url=forgot_password_url,
        )

    def forgot_password_template(self, redirect):
        forgot_password_url = url_for("admin_forgot_password")
        return self.FORGOT_PASSWORD_TEMPLATE % dict(
            redirect=redirect, forgot_password_url=forgot_password_url
        )

    def reset_password_template(self, reset_password_token, admin_id, redirect):
        reset_password_url = url_for(
            "admin_reset_password",
            reset_password_token=reset_password_token,
            admin_id=admin_id,
        )
        return self.RESET_PASSWORD_TEMPLATE % dict(
            redirect=redirect, reset_password_url=reset_password_url
        )

    def sign_in(self, _db, request={}):
        email = request.get("email")
        password = request.get("password")
        redirect_url = request.get("redirect")
        if redirect_url in (None, "None", "null"):
            redirect_url = "/admin/web"

        if email and password:
            match = Admin.authenticate(_db, email, password)
            if match:
                return (
                    dict(
                        email=match.email,
                        type=self.NAME,
                    ),
                    redirect_url,
                )

        return INVALID_ADMIN_CREDENTIALS, None

    def active_credentials(self, admin):
        # Admins who have a password are always active.
        return True

    def generate_reset_password_token(self, admin: Admin, _db: Session) -> str:
        secret_key = ConfigurationSetting.sitewide_secret(_db, Configuration.SECRET_KEY)

        reset_password_token = admin.generate_reset_password_token(secret_key)

        return reset_password_token

    def send_reset_password_email(self, admin: Admin, reset_password_url: str) -> None:
        subject = f"{AdminClientConfig.APP_NAME} - Reset password email"
        receivers = [admin.email]

        mail_text = render_template_string(
            reset_password_email_text,
            app_name=AdminClientConfig.APP_NAME,
            reset_password_url=reset_password_url,
        )
        mail_html = render_template_string(
            reset_password_email_html,
            app_name=AdminClientConfig.APP_NAME,
            reset_password_url=reset_password_url,
        )

        EmailManager.send_email(subject, receivers, text=mail_text, html=mail_html)

    def validate_token_and_extract_admin(
        self, reset_password_token: str, admin_id: int, _db: Session
    ) -> Union[Admin, ProblemDetail]:
        secret_key = ConfigurationSetting.sitewide_secret(_db, Configuration.SECRET_KEY)

        return Admin.validate_reset_password_token_and_fetch_admin(
            reset_password_token, admin_id, _db, secret_key
        )
