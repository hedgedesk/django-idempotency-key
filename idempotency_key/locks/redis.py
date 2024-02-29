from redis import Redis

from idempotency_key import utils
from idempotency_key.locks.basic import IdempotencyKeyLock


class MultiProcessRedisLock(IdempotencyKeyLock):
    """
    Should be used if a lock is required across processes. Note that this class uses
    Redis in order to perform the lock.
    """

    def __init__(self):
        location = utils.get_lock_location()
        if location is None or location == "":
            raise ValueError("Redis server location must be set in the settings file.")

        self.redis_obj = Redis.from_url(location)
        self.storage_lock = self.redis_obj.lock(
            name=utils.get_lock_name(),
            # Time before lock is forcefully released.
            timeout=utils.get_lock_time_to_live(),
            blocking_timeout=utils.get_lock_timeout(),
        )

    def acquire(self, *args, **kwargs) -> bool:
        return self.storage_lock.acquire()

    def release(self):
        self.storage_lock.release()


class MultiProcessRedisKeyLock(IdempotencyKeyLock):
    """
    Should be used if a lock is required across processes. Note that this class uses
    Redis in order to perform the lock.
    """

    redis_obj = None # singleton connection
    def __init__(self, idempotency_key=None):

        if not self.redis_obj:
            location = utils.get_lock_location()
            #if location is None or location == "":
            #    raise ValueError("Redis server location must be set in the settings file.")
            if location:
                MultiProcessRedisKeyLock.redis_obj = Redis.from_url(location)

        if self.redis_obj:
            lock_name = (
                f"{utils.get_lock_name()}-{idempotency_key}"
                if idempotency_key
                else utils.get_lock_name()
            )
            self.storage_lock = self.redis_obj.lock(
                name=lock_name,
                # Time before lock is forcefully released.
                timeout=utils.get_lock_time_to_live(),
                blocking_timeout=utils.get_lock_timeout(),
            )

    def is_alive(self):
        """
        Deprecated for now.
        """
        try:
            self.redis_obj.ping()
            return True
        except:
            return False

    def acquire(self, *args, **kwargs) -> bool:
        return self.storage_lock.acquire(blocking=False)

    def release(self):
        # Just incase if Middleware releases without acquiring. Less likely. Ignoring it instead of popping up an error.
        with suppress(Exception):
            self.storage_lock.release()