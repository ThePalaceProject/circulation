import re
from unittest import mock

import flask
from werkzeug import Response as WerkzeugResponse
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.problem_details import (
    ADMIN_AUTH_MECHANISM_NOT_CONFIGURED,
    ADMIN_AUTH_NOT_CONFIGURED,
    INVALID_ADMIN_CREDENTIALS,
)
from core.util.problem_detail import ProblemDetail
from tests.fixtures.api_admin import AdminControllerFixture


class TestResetPasswordController:
    def test_forgot_password_get(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller

        # If there is no admin with password then there is no auth providers and we should get error response
        admin_ctrl_fixture.admin.password_hashed = None
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/forgot_password"):
            assert [] == reset_password_ctrl.admin_auth_providers

            response = reset_password_ctrl.forgot_password()
            assert isinstance(response, ProblemDetail)

            assert response.status_code == 500
            assert response.uri == ADMIN_AUTH_NOT_CONFIGURED.uri

        # If auth providers are set we should get forgot password page - success path
        admin_ctrl_fixture.admin.password = "password"
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/forgot_password"):
            response = reset_password_ctrl.forgot_password()
            assert not isinstance(response, ProblemDetail)

            assert response.status_code == 200
            assert "Send reset password email" in response.get_data(as_text=True)

        assert isinstance(admin_ctrl_fixture.admin.email, str)
        # If admin is already signed in it gets redirected since it can use regular reset password flow
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/forgot_password"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", admin_ctrl_fixture.admin.email),
                    ("password", "password"),
                    ("redirect", "foo"),
                ]
            )
            sign_in_response = (
                admin_ctrl_fixture.manager.admin_sign_in_controller.password_sign_in()
            )

            # Check that sign in is successful
            assert sign_in_response.status_code == 302
            assert "foo" == sign_in_response.headers["Location"]

            response = reset_password_ctrl.forgot_password()
            assert response.status_code == 302
            assert not isinstance(response, ProblemDetail)

            location = response.headers.get("Location")
            assert isinstance(location, str)

            assert "admin/web" in location

    def test_forgot_password_post(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller

        # If there is no admin sent in the request we should get error response
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/forgot_password", method="POST"
        ):
            flask.request.form = ImmutableMultiDict([])

            response = reset_password_ctrl.forgot_password()
            assert isinstance(response, WerkzeugResponse)

            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code
            assert str(INVALID_ADMIN_CREDENTIALS.detail) in response.get_data(
                as_text=True
            )

        # If the admin does not exist we should also get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/forgot_password", method="POST"
        ):
            flask.request.form = ImmutableMultiDict([("email", "fake@admin.com")])

            response = reset_password_ctrl.forgot_password()
            assert isinstance(response, WerkzeugResponse)

            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code
            assert str(INVALID_ADMIN_CREDENTIALS.detail) in response.get_data(
                as_text=True
            )

        # When the real admin is used the email is sent and we get success message in the response
        with mock.patch(
            "api.admin.password_admin_authentication_provider.EmailManager"
        ) as mock_email_manager:
            with admin_ctrl_fixture.ctrl.app.test_request_context(
                "/admin/forgot_password", method="POST"
            ):
                admin_email = admin_ctrl_fixture.admin.email
                assert isinstance(admin_email, str)

                flask.request.form = ImmutableMultiDict([("email", admin_email)])

                response = reset_password_ctrl.forgot_password()
                assert isinstance(response, WerkzeugResponse)

                assert response.status_code == 200
                assert "Email successfully sent" in response.get_data(as_text=True)

                # Check the email is sent
                assert mock_email_manager.send_email.call_count == 1

                call_args, call_kwargs = mock_email_manager.send_email.call_args_list[0]

                # Check that the email is sent to the right admin
                _, receivers = call_args

                assert len(receivers) == 1
                assert receivers[0] == admin_email

    def test_reset_password_get(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller
        token = "token"

        admin_id = admin_ctrl_fixture.admin.id
        assert isinstance(admin_id, int)

        # If there is no admin with password then there is no auth providers and we should get error response
        admin_ctrl_fixture.admin.password_hashed = None
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            assert [] == reset_password_ctrl.admin_auth_providers

            response = reset_password_ctrl.reset_password(token, admin_id)
            assert isinstance(response, WerkzeugResponse)

            assert (
                response.status_code == ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.status_code
            )
            assert str(ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.detail) in response.get_data(
                as_text=True
            )

        # If admin is already signed in it gets redirected since it can use regular reset password flow
        admin_ctrl_fixture.admin.password = "password"

        admin_email = admin_ctrl_fixture.admin.email
        assert isinstance(admin_email, str)

        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", admin_email),
                    ("password", "password"),
                    ("redirect", "foo"),
                ]
            )
            sign_in_response = (
                admin_ctrl_fixture.manager.admin_sign_in_controller.password_sign_in()
            )

            # Check that sign in is successful
            assert sign_in_response.status_code == 302
            assert "foo" == sign_in_response.headers["Location"]

            response = reset_password_ctrl.reset_password(token, admin_id)
            assert isinstance(response, WerkzeugResponse)

            assert response.status_code == 302

            location = response.headers.get("Location")
            assert isinstance(location, str)
            assert "admin/web" in location

        # If we use bad token we get an error response with "Try again" button
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            response = reset_password_ctrl.reset_password(token, admin_id)
            assert isinstance(response, WerkzeugResponse)

            assert response.status_code == 401

            assert "Try again" in response.get_data(as_text=True)

        # If we use bad admin id we get an error response with "Try again" button
        bad_admin_id = admin_id + 1
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            response = reset_password_ctrl.reset_password(token, bad_admin_id)
            assert isinstance(response, WerkzeugResponse)

            assert response.status_code == 401

            assert "Try again" in response.get_data(as_text=True)

        # Finally, if we use good token we get back view with the form for the new password
        # Let's get valid token first
        with mock.patch(
            "api.admin.password_admin_authentication_provider.EmailManager"
        ) as mock_email_manager:
            with admin_ctrl_fixture.ctrl.app.test_request_context(
                "/admin/forgot_password", method="POST"
            ):
                flask.request.form = ImmutableMultiDict([("email", admin_email)])

                forgot_password_response = reset_password_ctrl.forgot_password()
                assert isinstance(forgot_password_response, WerkzeugResponse)

                assert forgot_password_response.status_code == 200

                call_args, call_kwargs = mock_email_manager.send_email.call_args_list[0]
                mail_text = call_kwargs["text"]

                (
                    token,
                    admin_id,
                ) = self._extract_reset_pass_token_and_admin_id_from_mail_text(
                    mail_text
                )

        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            assert isinstance(admin_id, int)
            response = reset_password_ctrl.reset_password(token, admin_id)
            assert isinstance(response, WerkzeugResponse)

            assert response.status_code == 200

            response_body = response.get_data(as_text=True)
            assert "New Password" in response_body
            assert "Confirm New Password" in response_body

    def _extract_reset_pass_token_and_admin_id_from_mail_text(self, mail_text):
        # Reset password url is in form of http[s]://url/admin/forgot_password/token
        reset_pass_url = re.search("(?P<url>https?://[^\\s]+)", mail_text).group("url")
        reset_pass_url_components = reset_pass_url.split("/")

        admin_id = int(reset_pass_url_components[-1])
        token = reset_pass_url_components[-2]

        return token, admin_id

    def test_reset_password_post(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller

        admin_email = admin_ctrl_fixture.admin.email
        assert isinstance(admin_email, str)

        # Let's get valid token first
        with mock.patch(
            "api.admin.password_admin_authentication_provider.EmailManager"
        ) as mock_email_manager:
            with admin_ctrl_fixture.ctrl.app.test_request_context(
                "/admin/forgot_password", method="POST"
            ):
                flask.request.form = ImmutableMultiDict([("email", admin_email)])

                response = reset_password_ctrl.forgot_password()
                assert response.status_code == 200

                call_args, call_kwargs = mock_email_manager.send_email.call_args_list[0]
                mail_text = call_kwargs["text"]

                (
                    token,
                    admin_id,
                ) = self._extract_reset_pass_token_and_admin_id_from_mail_text(
                    mail_text
                )

        # If we use bad token we get an error response with "Try again" button
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            reset_password_response = reset_password_ctrl.reset_password(
                "bad_token", admin_id
            )
            assert isinstance(reset_password_response, WerkzeugResponse)

            assert reset_password_response.status_code == 401
            assert "Try again" in reset_password_response.get_data(as_text=True)

        # If we use bad admin id we get an error response with "Try again" button
        bad_admin_id = admin_id + 1
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            reset_password_response = reset_password_ctrl.reset_password(
                token, bad_admin_id
            )
            assert isinstance(reset_password_response, WerkzeugResponse)

            assert reset_password_response.status_code == 401
            assert "Try again" in reset_password_response.get_data(as_text=True)

        # If there is no passwords we get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = ImmutableMultiDict([])

            reset_password_response = reset_password_ctrl.reset_password(
                token, admin_id
            )
            assert isinstance(reset_password_response, WerkzeugResponse)
            assert (
                reset_password_response.status_code
                == INVALID_ADMIN_CREDENTIALS.status_code
            )

        # If there is only one password we get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = ImmutableMultiDict([("password", "only_one")])

            reset_password_response = reset_password_ctrl.reset_password(
                token, admin_id
            )
            assert isinstance(reset_password_response, WerkzeugResponse)
            assert (
                reset_password_response.status_code
                == INVALID_ADMIN_CREDENTIALS.status_code
            )

        # If there are both passwords but they do not match we also get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = ImmutableMultiDict(
                [("password", "something"), ("confirm_password", "something_different")]
            )

            reset_password_response = reset_password_ctrl.reset_password(
                token, admin_id
            )
            assert isinstance(reset_password_response, WerkzeugResponse)
            assert (
                reset_password_response.status_code
                == INVALID_ADMIN_CREDENTIALS.status_code
            )

        # Finally, let's change that password!
        # Check current password
        assert admin_ctrl_fixture.admin.has_password("password")

        new_password = "new_password"
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = ImmutableMultiDict(
                [("password", new_password), ("confirm_password", new_password)]
            )

            reset_password_response = reset_password_ctrl.reset_password(
                token, admin_id
            )
            assert isinstance(reset_password_response, WerkzeugResponse)
            assert reset_password_response.status_code == 200

            assert admin_ctrl_fixture.admin.has_password(new_password)
