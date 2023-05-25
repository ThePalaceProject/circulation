from api.authentication.access_token import AccessTokenProvider
from core.scripts import Script


class RotateJWEKeyScript(Script):
    def do_run(self):
        current = AccessTokenProvider.get_current_key(self._db, create=False)
        self.log.info(
            f"Rotating out key {current and current.get('kid')}: {current and current.thumbprint()}"
        )

        new_key = AccessTokenProvider.rotate_key(self._db)
        self.log.info(f"Rotated new key {new_key.get('kid')}: {new_key.thumbprint()}")

        self._db.commit()
