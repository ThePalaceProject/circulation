from flask import Response, render_template, url_for
from sqlalchemy.orm.session import Session
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.wrappers.response import Response as WerkzeugResponse

from palace.manager.api.admin.admin_authentication_provider import (
    AdminAuthenticationProvider,
)
from palace.manager.api.admin.config import Configuration as AdminClientConfig
from palace.manager.api.admin.problem_details import INVALID_ADMIN_CREDENTIALS
from palace.manager.api.admin.template_styles import (
    button_style,
    input_style,
    label_style,
)
from palace.manager.service.email.email import SendEmailCallable
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.key import Key, KeyType
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail


class PasswordAdminAuthenticationProvider(AdminAuthenticationProvider, LoggerMixin):
    NAME = "Password Auth"

    def __init__(self, send_email: SendEmailCallable):
        self.send_email = send_email

    @staticmethod
    def get_secret_key(db: Session) -> str:
        return Key.get_key(db, KeyType.ADMIN_SECRET_KEY, raise_exception=True).value

    def sign_in_template(self, redirect: str | None) -> str:
        password_sign_in_url = url_for("password_auth")
        forgot_password_url = url_for("admin_forgot_password")
        return render_template(
            "admin/auth/sign-in-form.html.jinja2",
            redirect=redirect,
            password_sign_in_url=password_sign_in_url,
            forgot_password_url=forgot_password_url,
            support_contact_url=AdminClientConfig.admin_client_settings().support_contact_url,
            support_contact_text=AdminClientConfig.admin_client_settings().support_contact_text,
            label_style=label_style,
            input_style=input_style,
            button_style=button_style,
        )

    @staticmethod
    def forgot_password_template(
        redirect: str | None | Response | WerkzeugResponse,
    ) -> str:
        forgot_password_url = url_for("admin_forgot_password")
        return render_template(
            "admin/auth/forgot-password.html.jinja2",
            redirect=redirect,
            forgot_password_url=forgot_password_url,
            label_style=label_style,
            input_style=input_style,
            button_style=button_style,
        )

    @staticmethod
    def reset_password_template(
        reset_password_token: str,
        admin_id: int,
        redirect: str | None | Response | WerkzeugResponse,
    ) -> str:
        reset_password_url = url_for(
            "admin_reset_password",
            reset_password_token=reset_password_token,
            admin_id=admin_id,
        )
        return render_template(
            "admin/auth/reset-password-form.html.jinja2",
            reset_password_url=reset_password_url,
            redirect=redirect,
            label_style=label_style,
            input_style=input_style,
            button_style=button_style,
        )

    def sign_in(
        self,
        _db: Session,
        request: ImmutableMultiDict[str, str] | dict[str, str | None] | None = None,
    ) -> tuple[dict[str, str], str | None] | tuple[ProblemDetail, None]:
        if request is None:
            request = {}
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

    def active_credentials(self, admin: Admin) -> bool:
        # Admins who have a password are always active.
        return True

    def generate_reset_password_token(self, admin: Admin, _db: Session) -> str:
        secret_key = self.get_secret_key(_db)

        reset_password_token = admin.generate_reset_password_token(secret_key)

        return reset_password_token

    def send_reset_password_email(self, admin: Admin, reset_password_url: str) -> None:
        subject = f"{AdminClientConfig.APP_NAME} - Reset password email"
        receivers = [admin.email]

        mail_text = render_template(
            "admin/email/reset-password.text.jinja2",
            app_name=AdminClientConfig.APP_NAME,
            reset_password_url=reset_password_url,
        )

        mail_html = render_template(
            "admin/email/reset-password.html.jinja2",
            app_name=AdminClientConfig.APP_NAME,
            reset_password_url=reset_password_url,
        )

        self.send_email(
            subject=subject, receivers=receivers, text=mail_text, html=mail_html
        )

    def validate_token_and_extract_admin(
        self, reset_password_token: str, admin_id: int, _db: Session
    ) -> Admin | ProblemDetail:
        secret_key = self.get_secret_key(_db)

        return Admin.validate_reset_password_token_and_fetch_admin(
            reset_password_token, admin_id, _db, secret_key
        )
