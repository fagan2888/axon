import redis


class RedisClient(object):
    EXPIRATION_TIME_IN_SECONDS = 60 * 60 * 24 * 2

    def __init__(self, host, port):
        self.client = redis.StrictRedis(host, port)

    def upload_to_redis(self, key, value):
        pipe = self.client.pipeline()
        if isinstance(value, dict):
            pipe.hmset(key, value)
        else:
            pipe.set(key, value)
        pipe.expire(key, self.EXPIRATION_TIME_IN_SECONDS)
        # for row in rows:
        #     pipe.set(KEY_HERE, VALUE_HERE)
        #     pipe.expire(KEY_HERE, self.EXPIRATION_TIME_IN_SECONDS)
        pipe.execute()

    def get_obj(self, key):
        return self.client.get(key)

    def get_hm_obj(self, key, inner_key):
        return self.client.hmget(key, inner_key)