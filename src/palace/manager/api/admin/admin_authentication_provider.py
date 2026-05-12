from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.admin import Admin


class AdminAuthenticationProvider:
    def sign_in_template(self, redirect_url: str | None) -> str:
        # Returns HTML to be rendered on the sign in page for
        # this authentication provider.
        raise NotImplementedError()

    def active_credentials(self, admin: Admin) -> bool:
        # Returns True if the admin's credentials are not expired.
        raise NotImplementedError()
