from api.authentication.access_token import PatronJWEAccessTokenProvider
from core.scripts import Script


class RotateJWEKeyScript(Script):
    def do_run(self) -> None:
        try:
            current = PatronJWEAccessTokenProvider.get_key(self._db)
            jwk = PatronJWEAccessTokenProvider.get_jwk(current)
            self.log.info(f"Rotating out key {current.id}: {jwk.thumbprint()}")
        except ValueError:
            self.log.info("No current key found")

        new_key = PatronJWEAccessTokenProvider.create_key(self._db)
        new_jwk = PatronJWEAccessTokenProvider.get_jwk(new_key)
        self.log.info(f"Rotated in key {new_key.id}: {new_jwk.thumbprint()}")

        # Remove old / expired keys
        removed = PatronJWEAccessTokenProvider.delete_old_keys(self._db)
        self.log.info(f"Removed {removed} expired keys")

        self._db.commit()
