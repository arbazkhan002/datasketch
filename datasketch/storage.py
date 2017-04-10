from collections import defaultdict
from abc import ABCMeta, abstractmethod
import redis
import random, string
from functools import reduce


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


class Storage(metaclass=ABCMeta):
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
        pass

    @abstractmethod
    def get(self, key):
        """Get list of values associated with a key
        
        Returns empty list ([]) if `key` is not found
        """
        pass

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
    def itemcounts(self):
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

    def itemcounts(self):
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
        return [x for x in self._redis.hkeys(self._name)]

    def redis_keys(self):
        return self._redis.hvals(self._name)

    def get(self, key):
        return self._redis.lrange(self.redis_key(key), 0, -1)

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

    def itemcounts(self):
        return {k: self._redis.llen(k) for k in self.redis_keys()}

    def has_key(self, key):
        return self._redis.hexists(self._name, key)


class RedisSetStorage(UnorderedStorage, RedisListStorage):

    def get(self, key):
        return self._redis.smembers(self.redis_key(key))

    def remove_val(self, key, val):
        redis_key = self.redis_key(key)
        self._redis.srem(redis_key, val)
        if not self._redis.exists(redis_key):
            self._redis.hdel(self._name, redis_key)

    def insert(self, key, val):
        redis_key = self.redis_key(key)
        self._redis.hset(self._name, key, redis_key)
        self._redis.sadd(redis_key, val)

    def itemcounts(self):
        return {k: self._redis.scard(self.redis_key(k)) for k in self.keys()}

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
