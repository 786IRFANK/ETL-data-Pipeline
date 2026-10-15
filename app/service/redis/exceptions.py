class RedisServiceError(Exception):
    pass


class JobNotFoundError(RedisServiceError):
    pass
