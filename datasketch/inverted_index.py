import tqdm

from datasketch.storage import (
    ordered_storage, unordered_storage)
from datasketch.minhash import MinHash


class InvertedIndex(object):
    """
    Simple inverted index to use in conjunction with LSH (e.g. for comparison).
    """
    def __init__(self, storage_config={'type':'dict'}, display_progress=True):
        self.display_progress = display_progress
        self.storage_config = storage_config

        self.index = unordered_storage(storage_config)
        self.keys = unordered_storage(storage_config)

    def insert(self, key, value):
        self.keys.insert(key, value)
        self.index.insert(value, key)

    def insert_documents(self, **documents):
        for key, document in documents.items():
            for word in document:
                self.insert(key.encode('utf8'), word.encode('utf8'))

    def query(self, value):
        return list(self.index.get(value))

    def __contains__(self, key):
        return key in self.keys

    def remove(self, key):
        """
        Remove the key from the index.

        Args:
            key (hashable): The unique identifier of a set.
        """
        if key not in self.keys:
            raise ValueError("The given key does not exist")
        values = self.keys[key]
        for value in values:
            self.index.remove_val(value, key)
            if not self.index.get(value):
                self.index.remove(value)
        self.keys.remove(key)

    def is_empty(self):
        """
        Returns:
            bool: Check if the index is empty.
        """
        return self.index.size() == 0

    def get_counts(self, **kwargs):
        self.counts_ = self.index.itemcounts()
        return self.counts_

    def __getstate__(self):
        state = self.__dict__.copy()
        # Do not persist counts
        state.pop('counts_', None)
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        # self.get_counts()

    def query_keys(self, *keys):
        keys = [x.encode('utf8') for x in keys]
        values = {v for value in self.keys.getmany(*keys) for v in value}
        to_union = {self.index.get_reference(v) for v in values}
        r = self.index.union_references(*to_union)
        for key in keys:
            r.discard(key)
        l = [x.decode('utf8') for x in r]
        return l

    def get_subset_counts(self, *keys):
        key_set = list(set(x.encode('utf8') for x in keys))
        hashtable = unordered_storage({'type': 'dict'})
        values = self.keys.getmany(*key_set)
        for key, value in zip(key_set, values):
            for v in value:
                hashtable.insert(v, key)
        return hashtable.itemcounts()

    def get_values(self, *keys):
        return {v for value in self.keys.getmany(
            *[key.encode('utf8') for key in keys]) for v in value}

    def get_status(self):
        return self.keys.status()
