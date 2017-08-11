import unittest
import pickle
import mockredis
from mock import patch
from datasketch import InvertedIndex


def fake_redis(**kwargs):
    redis = mockredis.mock_redis_client(**kwargs)
    redis.connection_pool = None
    redis.response_callbacks = None
    return redis


class TestInvertedIndex(unittest.TestCase):

    def test_init(self):
        index = InvertedIndex()
        self.assertTrue(index.is_empty())

    def test_insert(self):
        index = InvertedIndex()
        index.insert("a", "a")
        index.insert("b", "b")
        self.assertTrue(len(index.index) >= 1)
        items = []
        for v in index.index:
            items.extend(v)
        self.assertTrue("a" in items)
        self.assertTrue("b" in items)
        self.assertTrue("a" in index)
        self.assertTrue("b" in index)
        for v in index.keys["a"]:
            self.assertTrue("a" in index.index[v])

    def test_query(self):
        index = InvertedIndex()
        index.insert("a", "a")
        index.insert("b", "b")
        result = index.query("a")
        self.assertTrue("a" in result)
        result = index.query("b")
        self.assertTrue("b" in result)

    def test_remove(self):
        index = InvertedIndex()
        index.insert("a", "a")
        index.insert("b", "b")
        index.remove("a")
        self.assertTrue("a" not in index.keys)
        for table in index.index:
            self.assertGreater(len(table), 0)
            self.assertTrue("a" not in table)

        self.assertRaises(ValueError, index.remove, "c")

    def test_pickle(self):
        index = InvertedIndex()
        index.insert("a", "a")
        index.insert("b", "b")
        index2 = pickle.loads(pickle.dumps(index))
        result = index2.query("a")
        self.assertTrue("a" in result)
        result = index2.query("b")
        self.assertTrue("b" in result)

    def test_insert_redis(self):
        with patch('redis.Redis', fake_redis) as mock_redis:
            index = InvertedIndex(storage_config={
                'type': 'redis', 'redis': {'host': 'localhost', 'port': 6379}
            })
            index.insert(b"a", b"a")
            index.insert(b"b", b"b")
            self.assertTrue(len(index.index) >= 1)
            self.assertTrue(b"a" in index.keys[b"a"])
            self.assertTrue(b"b" in index.keys[b"b"])
            for v in index.keys[b"a"]:
                self.assertTrue(b"a" in index.index[v])

    def test_query_redis(self):
        with patch('redis.Redis', fake_redis) as mock_redis:
            index = InvertedIndex(storage_config={
                'type': 'redis', 'redis': {'host': 'localhost', 'port': 6379}
            })
            index.insert(b"a", b"a")
            index.insert(b"b", b"b")
            result = index.query(b"a")
            self.assertTrue(b"a" in result)
            result = index.query(b"b")
            self.assertTrue(b"b" in result)

    def test_insertion_session(self):
        index = InvertedIndex()
        with index.insertion_session() as session:
            session.insert("a", "a")
            session.insert("b", "b")
        self.assertTrue(len(index.index) >= 1)
        items = []
        for v in index.index:
            items.extend(v)
        self.assertTrue("a" in items)
        self.assertTrue("b" in items)
        self.assertTrue("a" in index)
        self.assertTrue("b" in index)
        for v in index.keys["a"]:
            self.assertTrue("a" in index.index[v])

    def test_get_counts(self):
        index = InvertedIndex()
        index.insert("a", "a")
        index.insert("b", "b")
        counts = index.get_counts()
        self.assertEqual(counts, {"a": 1, "b": 1})


if __name__ == "__main__":
    unittest.main()
