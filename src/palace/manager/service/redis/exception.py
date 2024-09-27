from palace.manager.core.exceptions import BasePalaceException


class RedisKeyError(BasePalaceException, TypeError): ...


class RedisValueError(BasePalaceException, ValueError): ...
