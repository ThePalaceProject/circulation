from __future__ import annotations

from urllib.parse import quote_plus

import flask
from flask import Response, redirect, url_for
from flask_babel import lazy_gettext as _

from api.admin.config import Configuration as AdminClientConfig
from api.admin.controller.base import AdminController
from api.admin.templates import admin as admin_template
from api.config import Configuration
from core.model import ConfigurationSetting, Library
from core.util.problem_detail import ProblemDetail


class ViewController(AdminController):
    def __call__(self, collection, book, path=None):
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

        csrf_token = (
            flask.request.cookies.get("csrf_token") or self.generate_csrf_token()
        )
        admin_js = AdminClientConfig.lookup_asset_url(key="admin_js")
        admin_css = AdminClientConfig.lookup_asset_url(key="admin_css")

        # Find the URL and text to use when rendering the Terms of
        # Service link in the footer.
        sitewide_tos_href = (
            ConfigurationSetting.sitewide(self._db, Configuration.CUSTOM_TOS_HREF).value
            or Configuration.DEFAULT_TOS_HREF
        )

        sitewide_tos_text = (
            ConfigurationSetting.sitewide(self._db, Configuration.CUSTOM_TOS_TEXT).value
            or Configuration.DEFAULT_TOS_TEXT
        )

        # We always have local_analytics
        show_circ_events_download = True

        response = Response(
            flask.render_template_string(
                admin_template,
                app_name=AdminClientConfig.APP_NAME,
                csrf_token=csrf_token,
                sitewide_tos_href=sitewide_tos_href,
                sitewide_tos_text=sitewide_tos_text,
                show_circ_events_download=show_circ_events_download,
                setting_up=setting_up,
                email=email,
                roles=roles,
                admin_js=admin_js,
                admin_css=admin_css,
            )
        )

        # The CSRF token is in its own cookie instead of the session cookie,
        # because if your session expires and you log in again, you should
        # be able to submit a form you already had open. The CSRF token lasts
        # until the user closes the browser window.
        response.set_cookie("csrf_token", csrf_token, httponly=True)
        return response
