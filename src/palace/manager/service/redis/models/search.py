from collections.abc import Sequence

from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.log import LoggerMixin


class WaitingForIndexing(LoggerMixin):
    def __init__(
        self,
        redis_client: Redis,
    ):
        self._redis_client = redis_client
        self._key = self._redis_client.get_key("Search", self.__class__.__name__)

    def add(self, work: Work | int | None) -> bool:
        work_id = work.id if isinstance(work, Work) else work
        if work_id is None:
            self.log.warning(f"Attempted to add None to {self.__class__.__name__}.")
            return False

        return self._redis_client.sadd(self._key, work_id) == 1

    def pop(self, size: int) -> Sequence[int]:
        elements = self._redis_client.spop(self._key, size)
        return [int(e) for e in elements]

    def get(self, size: int) -> Sequence[int]:
        elements = self._redis_client.srandmember(self._key, size)
        return [int(e) for e in elements]

    def remove(self, works: Sequence[int]) -> None:
        if len(works) == 0:
            return
        self._redis_client.srem(self._key, *works)
