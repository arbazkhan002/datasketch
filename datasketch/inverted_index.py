from datasketch.storage import unordered_storage


class InvertedIndex(object):
    '''
    Simple inverted index to use in conjunction with LSH (e.g. for comparison).

        Args:
            storage_config (dict, optional): Type of storage service to use for storing
                hashtables and keys.
    '''
    def __init__(self, storage_config={'type':'dict'}):
        self.storage_config = storage_config

        self.index = unordered_storage(storage_config)
        self.keys = unordered_storage(storage_config)

    def insert(self, key, value):
        '''
        Insert a key, value pair to the index. If the key is already present,
        the value is treated as an addition.

        Args:
            key (hashable): The unique identifier of the set.
            value (hashable): The value to insert under the given key.
        '''
        self._insert(key, value)

    def insertion_session(self):
        '''
        Create a context manager for fast insertion into this index.

        Returns:
            datasketch.inverted_index.InvertedIndexInsertionSession
        '''
        return InvertedIndexInsertionSession(self)

    def _insert(self, key, value, buffer=False):
        self.keys.insert(key, value, buffer=buffer)
        self.index.insert(value, key, buffer=buffer)

    def query(self, value):
        '''
        Giving a value, this returns all keys which were present with
        that specified value.

        Args:
            value (hashable): The value to query.

        Returns:
            `list` of keys.
        '''
        return list(self.index.get(value))

    def __contains__(self, key):
        '''
        Args:
            key (hashable): The unique identifier of a set.

        Returns:
            bool: True only if the key exists in the index.
        '''
        return key in self.keys

    def remove(self, key):
        '''
        Remove the key from the index.

        Args:
            key (hashable): The unique identifier of a set.
        '''
        if key not in self.keys:
            raise ValueError("The given key does not exist")
        values = self.keys[key]
        for value in values:
            self.index.remove_val(value, key)
            if not self.index.get(value):
                self.index.remove(value)
        self.keys.remove(key)

    def is_empty(self):
        '''
        Returns:
            bool: Check if the index is empty.
        '''
        return self.index.size() == 0

    def get_counts(self, **kwargs):
        return self.index.itemcounts(**kwargs)

    def get_subset_counts(self, *keys):
        key_set = list(set(keys))
        index = unordered_storage({'type': 'dict'})
        values = self.keys.getmany(*key_set)
        for key, value in zip(key_set, values):
            for v in value:
                index.insert(v, key)
        return index.itemcounts()


class InvertedIndexInsertionSession:
    '''Context manager for batch insertion of documents into an InvertedIndex.
    '''

    def __init__(self, ii):
        self.ii = ii

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ii.keys.empty_buffer()
        self.ii.index.empty_buffer()

    def insert(self, key, value):
        '''
        Insert a unique key to the index, together
        with a MinHash (or weighted MinHash) of the set referenced by
        the key.

        Args:
            key (hashable): The unique identifier of the set.
            value: the value to be inserted
        '''
        self.ii._insert(key, value, buffer=True)
