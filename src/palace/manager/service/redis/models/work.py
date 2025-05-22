from datetime import timedelta

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.redis.models.set import RedisSet
from palace.manager.service.redis.redis import Redis


class WorkIdAndPolicy(BaseFrozenData):
    work_id: int
    policy: PresentationCalculationPolicy


class WaitingForPresentationCalculation(RedisSet[WorkIdAndPolicy]):
    def __init__(
        self,
        redis_client: Redis,
        expire_time: timedelta | int = timedelta(hours=12),
    ):
        super().__init__(
            redis_client=redis_client,
            model_cls=WorkIdAndPolicy,
            key="PresentationCalculation",
            expire_time=expire_time,
        )
