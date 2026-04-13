from palace.util.exceptions import BasePalaceException


class RedisKeyError(BasePalaceException, TypeError): ...


class RedisValueError(BasePalaceException, ValueError): ...
