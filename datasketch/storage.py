from collections import defaultdict
import redis
import random, string
from functools import reduce
from abc import ABCMeta, abstractmethod
ABC = ABCMeta('ABC', (object,), {}) # compatible with Python 2 *and* 3

def ordered_storage(config):
    """Return ordered storage system based on the specified config"""
    tp = config['type']
    if tp == 'dict':
        return DictListStorage(config)
    if tp == 'redis':
        return RedisListStorage(config)


def unordered_storage(config):
    """Return an unordered storage system based on the specified config"""
    tp = config['type']
    if tp == 'dict':
        return DictSetStorage(config)
    if tp == 'redis':
        return RedisSetStorage(config)


def prepare_storage(config):
    tp = config['type']
    if tp == 'redis':
        redis_driver = redis.Redis(**config['redis'])
        redis_driver.flushdb()


# def sorted_storage(config):
#     tp = config['type']
#     if tp == 'dict':
#         return SortedDict(config)
#     if tp == 'redis':
#         return RedisSortedStorage(config)


class Storage(ABC):
    def __getitem__(self, key):
        return self.get(key)

    def __delitem__(self, key):
        return self.remove(key)

    def __len__(self):
        return self.size()

    def __iter__(self):
        for key in self.keys():
            yield key

    def __contains__(self, item):
        return self.has_key(item)

    @abstractmethod
    def keys(self):
        """Return an iterator on keys in storage"""
        return []

    @abstractmethod
    def get(self, key):
        """Get list of values associated with a key
        
        Returns empty list ([]) if `key` is not found
        """
        pass

    def getmany(self, *keys):
        return [self.get(key) for key in keys]

    def get_reference(self, key):
        return self.get(key)

    @abstractmethod
    def insert(self, key, val):
        """Add `val` to storage against `key`"""
        pass

    @abstractmethod
    def remove(self, key):
        """Remove `key` from storage"""
        pass

    @abstractmethod
    def remove_val(self, key, val):
        """Remove `val` from list of values under `key`"""
        pass

    @abstractmethod
    def size(self):
        """Return size of storage with respect to number of keys"""
        pass

    @abstractmethod
    def itemcounts(self, **kwargs):
        """Returns the number of items stored under each key"""
        pass

    @abstractmethod
    def has_key(self, key):
        """Determines whether the key is in the storage or not"""
        pass


class OrderedStorage(Storage):

    pass


class UnorderedStorage(Storage):

    @abstractmethod
    def union_references(self, *references):
        pass


class DictListStorage(OrderedStorage):
    def __init__(self, config):
        self._dict = defaultdict(list)

    def keys(self):
        return self._dict.keys()

    def get(self, key):
        return self._dict.get(key, [])

    def get_reference(self, key):
        return tuple(self._dict.get(key, []))

    def remove(self, key):
        del self._dict[key]

    def remove_val(self, key, val):
        self._dict[key].remove(val)

    def insert(self, key, val):
        self._dict[key].append(val)

    def size(self):
        return len(self._dict)

    def itemcounts(self, **kwargs):
        return {k: len(v) for k, v in self._dict.items()}

    def has_key(self, key):
        return key in self._dict


class DictSetStorage(UnorderedStorage, DictListStorage):
    def __init__(self, config):
        self._dict = defaultdict(set)

    def get(self, key):
        return self._dict.get(key, set())

    def get_reference(self, key):
        return frozenset(self._dict.get(key, frozenset()))

    def insert(self, key, val):
        self._dict[key].add(val)

    def union_references(self, *references):
        return set(reduce(lambda a, b: a | b, references))


# class SortedDict:
#
#     def __init__(self, config):
#         self._dict = defaultdict(lambda: 0)
#
#     def increment(self, key, value):
#         self._dict[key] += value
#
#     def clear(self):
#         self._dict.clear()
#
#     def __getitem__(self, item):
#         st = sorted(self._dict.items(), key=lambda x: x[1], reverse=True)
#         return st[item]
#
#     def __len__(self):
#         return len(self._dict)
#
#     def __iter__(self):
#         return iter(self._dict.items())
#
#     def remove(self, key):
#         del self._dict[key]


class RedisStorage:

    def __init__(self, config, name=None):
        self.config = config
        self._redis = redis.Redis(**self.config['redis'])
        if name is None:
            name = _random_name(11)
        self._name = name

    def redis_key(self, key):
        return (self._name, key)

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('_redis')
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._redis = redis.Redis(**self.config['redis'])


class RedisListStorage(OrderedStorage, RedisStorage):

    def keys(self):
        return self._redis.hkeys(self._name)

    def redis_keys(self):
        return self._redis.hvals(self._name)

    def get(self, key):
        return self._get_items(self._redis, self.redis_key(key))

    def getmany(self, *keys):
        pipe = self._redis.pipeline()
        pipe.multi()
        for key in keys:
            pipe.lrange(self.redis_key(key), 0, -1)
        return pipe.execute()

    @staticmethod
    def _get_items(r, k):
        return r.lrange(k, 0, -1)

    def get_reference(self, key):
        return self.redis_key(key)

    def remove(self, key):
        self._redis.hdel(self._name, key)
        self._redis.delete(self.redis_key(key))

    def remove_val(self, key, val):
        redis_key = self.redis_key(key)
        self._redis.lrem(redis_key, val)
        if not self._redis.exists(redis_key):
            self._redis.hdel(self._name, redis_key)

    def insert(self, key, val):
        self._redis.hset(self._name, key, self.redis_key(key))
        self._redis.rpush(self.redis_key(key), val)

    def size(self):
        return self._redis.hlen(self._name)

    def itemcounts(self, force=False, **kwargs):
        master_key = self.redis_key('counts')
        if not force and self._redis.exists(master_key):
            return {k: int(v)
                    for k, v in self._redis.hgetall(master_key).items()}
        pipe = self._redis.pipeline()
        pipe.multi()
        ks = self.keys()
        for k in ks:
            self._get_len(pipe, self.redis_key(k))
        d = dict(zip(ks, pipe.execute()))
        self._redis.hmset(master_key, d)
        return d

    @staticmethod
    def _get_len(r, k):
        return r.llen(k)

    def has_key(self, key):
        return self._redis.hexists(self._name, key)


class RedisSetStorage(UnorderedStorage, RedisListStorage):

    @staticmethod
    def _get_items(r, k):
        return r.smembers(k)

    def remove_val(self, key, val):
        redis_key = self.redis_key(key)
        self._redis.srem(redis_key, val)
        if not self._redis.exists(redis_key):
            self._redis.hdel(self._name, redis_key)

    def insert(self, key, val):
        redis_key = self.redis_key(key)
        self._redis.hset(self._name, key, redis_key)
        self._redis.sadd(redis_key, val)

    @staticmethod
    def _get_len(r, k):
        return r.scard(k)

    def union_references(self, *references):
        if not references:
            return set()
        return self._redis.sunion(references)


# class RedisSortedStorage(RedisStorage):
#
#     def increment_many(self, other, value):
#         self._redis.zunionstore(self._name, {self._name:1, other:value})
#
#     def increment(self, key, value):
#         self._redis.zincrby(self._name, key, amount=value)
#
#     def clear(self):
#         self._redis.delete(self._name)
#
#     def __getitem__(self, item):
#         if isinstance(item, slice):
#             start = item.start
#             if start is None: start = 0
#             stop = item.stop
#             if item.stop is None: stop = -1
#             return self._redis.zrevrange(
#                 self._name, start, stop, withscores=True)
#         else:
#             return self._redis.zrevrange(self._name, item, item + 1,
#                                          withscores=True)
#
#     def __iter__(self):
#         return iter(self._redis.zrevrange(self._name, 0, -1, withscores=True))
#
#     def __len__(self):
#         return self._redis.zcard(self._name)
#
#     def remove(self, key):
#         return self._redis.zrem(self._name, key)
#

def _random_name(length):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))
