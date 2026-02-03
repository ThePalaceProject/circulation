from __future__ import annotations

from urllib.parse import quote_plus

import flask
from flask import Response, redirect, url_for
from flask_babel import lazy_gettext as _
from werkzeug import Response as WerkzeugResponse

from palace.manager.api.admin.config import Configuration as AdminClientConfig
from palace.manager.api.admin.controller.base import AdminController
from palace.manager.api.config import Configuration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.problem_detail import ProblemDetail


class ViewController(AdminController):
    def __call__(
        self,
        collection: str | None,
        book: str | None,
        path: str | None = None,
    ) -> Response | WerkzeugResponse:
        setting_up = self.admin_auth_providers == []
        email = None
        roles = []
        if not setting_up:
            admin = self.authenticated_admin_from_request()
            if isinstance(admin, ProblemDetail):
                redirect_url = flask.request.url
                if collection:
                    redirect_url = redirect_url.replace(
                        collection, quote_plus(collection, safe="()")
                    )
                if book:
                    redirect_url = redirect_url.replace(
                        book, quote_plus(book, safe="()")
                    )
                return redirect(
                    url_for("admin_sign_in", redirect=redirect_url, _external=True)
                )

            if not collection and not book and not path:
                if self._db.query(Library).count() > 0:
                    # Find the first library the admin is a librarian of.
                    library_name = None
                    for library in self._db.query(Library).order_by(Library.id.asc()):
                        if admin.is_librarian(library):
                            library_name = library.short_name
                            break
                    if not library_name:
                        return Response(
                            _(
                                "Your admin account doesn't have access to any libraries. Contact your library manager for assistance."
                            ),
                            200,
                        )
                    return redirect(
                        url_for("admin_view", collection=library_name, _external=True)
                    )

            email = admin.email
            for role in admin.roles:
                if role.library:
                    roles.append({"role": role.role, "library": role.library})
                else:
                    roles.append({"role": role.role})

        # Check if CSRF token already exists and is valid, only generate if needed
        existing_csrf_token = flask.request.cookies.get("csrf_token")
        has_valid_token = existing_csrf_token and self.validate_csrf_token(
            existing_csrf_token
        )

        if has_valid_token:
            assert existing_csrf_token is not None
            csrf_token = existing_csrf_token
        else:
            csrf_token = self.generate_csrf_token()

        admin_js = AdminClientConfig.lookup_asset_url(key="admin_js")
        admin_css = AdminClientConfig.lookup_asset_url(key="admin_css")

        response = Response(
            flask.render_template(
                "admin/app-home-page.html.jinja2",
                app_name=AdminClientConfig.APP_NAME,
                csrf_token=csrf_token,
                sitewide_tos_href=Configuration.DEFAULT_TOS_HREF,
                sitewide_tos_text=Configuration.DEFAULT_TOS_TEXT,
                show_circ_events_download=AdminClientConfig.admin_feature_flags().show_circ_events_download,
                support_contact_url=AdminClientConfig.admin_client_settings().support_contact_url,
                support_contact_text=AdminClientConfig.admin_client_settings().support_contact_text,
                setting_up=setting_up,
                email=email,
                roles=roles,
                admin_js=admin_js,
                admin_css=admin_css,
                feature_flags=AdminClientConfig.admin_feature_flags().model_dump_json(
                    by_alias=True
                ),
            )
        )

        # The CSRF token is in its own cookie instead of the session cookie,
        # because if your session expires and you log in again, you should
        # be able to submit a form you already had open. The CSRF token lasts
        # until the user closes the browser window.
        # Only set the cookie if we generated a new token (not from user input).
        if not has_valid_token:
            response.set_cookie(
                "csrf_token",
                csrf_token,
                httponly=True,
                secure=not flask.current_app.debug,
                samesite="Lax",
            )
        return response
