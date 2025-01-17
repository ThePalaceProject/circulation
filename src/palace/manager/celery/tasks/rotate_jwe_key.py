from celery import shared_task

from palace.manager.api.authentication.access_token import PatronJWEAccessTokenProvider
from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames


@shared_task(queue=QueueNames.default, bind=True)
def rotate_jwe_key(task: Task) -> None:
    with task.transaction() as session:
        try:
            current = PatronJWEAccessTokenProvider.get_key(session)
            jwk = PatronJWEAccessTokenProvider.get_jwk(current)
            task.log.info(f"Rotating out key {current.id}: {jwk.thumbprint()}")
        except ValueError:
            task.log.warning("No patron JWE key found")

        new_key = PatronJWEAccessTokenProvider.create_key(session)
        new_jwk = PatronJWEAccessTokenProvider.get_jwk(new_key)
        task.log.info(f"Rotated in key {new_key.id}: {new_jwk.thumbprint()}")

        # Remove old / expired keys
        removed = PatronJWEAccessTokenProvider.delete_old_keys(session)
        task.log.info(f"Removed {removed} expired keys")
