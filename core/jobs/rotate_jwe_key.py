from api.authentication.access_token import PatronJWEAccessTokenProvider
from core.scripts import Script


class RotateJWEKeyScript(Script):
    def do_run(self):
        current = PatronJWEAccessTokenProvider.get_current_key(self._db, create=False)
        self.log.info(
            f"Rotating out key {current and current.key_id}: {current and current.thumbprint()}"
        )

        new_key = PatronJWEAccessTokenProvider.rotate_key(self._db)
        self.log.info(f"Rotated new key {new_key.key_id}: {new_key.thumbprint()}")

        self._db.commit()
