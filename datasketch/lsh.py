from datasketch.storage import (
    ordered_storage, unordered_storage)
from datasketch.minhash import MinHash


_integration_precision = 0.001
def _integration(f, a, b):
    p = _integration_precision
    area = 0.0
    x = a
    while x < b:
        area += f(x+0.5*p)*p
        x += p
    return area, None

try:
    from scipy.integrate import quad as integrate
except ImportError:
    # For when no scipy installed
    integrate = _integration


def _false_positive_probability(threshold, b, r):
    _probability = lambda s : 1 - (1 - s**float(r))**float(b)
    a, err = integrate(_probability, 0.0, threshold) 
    return a


def _false_negative_probability(threshold, b, r):
    _probability = lambda s : 1 - (1 - (1 - s**float(r))**float(b))
    a, err = integrate(_probability, threshold, 1.0)
    return a


def _optimal_param(threshold, num_perm, false_positive_weight,
        false_negative_weight):
    """
    Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
    of probabilities of false positive and false negative.
    """
    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm+1):
        max_r = int(num_perm / b)
        for r in range(1, max_r+1):
            fp = _false_positive_probability(threshold, b, r)
            fn = _false_negative_probability(threshold, b, r)
            error = fp*false_positive_weight + fn*false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt


class MinHashLSH(object):
    '''
    The :ref:`minhash_lsh` index. 
    It supports query with `Jaccard similarity`_ threshold.
    Reference: `Chapter 3, Mining of Massive Datasets 
    <http://www.mmds.org/>`_.

    Args:
        threshold (float): The Jaccard similarity threshold between 0.0 and
            1.0. The initialized MinHash LSH will be optimized for the threshold by
            minizing the false positive and false negative.
        num_perm (int, optional): The number of permutation functions used
            by the MinHash to be indexed. For weighted MinHash, this
            is the sample size (`sample_size`).
        weights (tuple, optional): Used to adjust the relative importance of 
            minizing false positive and false negative when optimizing 
            for the Jaccard similarity threshold.
            `weights` is a tuple in the format of 
            :code:`(false_positive_weight, false_negative_weight)`.
        params (tuple, optional): The LSH parameters (i.e., number of bands and size
            of each bands). This is used to bypass the parameter optimization
            step in the constructor. `threshold` and `weights` will be ignored 
            if this is given.

    Note: 
        `weights` must sum to 1.0, and the format is 
        (false positive weight, false negative weight).
        For example, if minizing false negative (or maintaining high recall) is more
        important, assign more weight toward false negative: weights=(0.4, 0.6).
        Try to live with a small difference between weights (i.e. < 0.5).
    '''

    def __init__(self, threshold=0.9, num_perm=128, weights=(0.5,0.5), params=None):
        if threshold > 1.0 or threshold < 0.0:
            raise ValueError("threshold must be in [0.0, 1.0]") 
        if num_perm < 2:
            raise ValueError("Too few permutation functions")
        if any(w < 0.0 or w > 1.0 for w in weights):
            raise ValueError("Weight must be in [0.0, 1.0]")
        if sum(weights) != 1.0:
            raise ValueError("Weights must sum to 1.0")
        self.h = num_perm
        if params is not None:
            self.b, self.r = params
            if self.b * self.r > num_perm:
                raise ValueError("The product of b and r must be less than num_perm")
        else:
            false_positive_weight, false_negative_weight = weights
            self.b, self.r = _optimal_param(threshold, num_perm,
                    false_positive_weight, false_negative_weight)
        self.hashtables = [defaultdict(list) for _ in range(self.b)]
        self.hashranges = [(i*self.r, (i+1)*self.r) for i in range(self.b)]
        self.keys = ordered_storage(storage_config)

    def insert(self, key, minhash):
        """
        Insert a unique key to the index, together
        with a MinHash (or weighted MinHash) of the set referenced by 
        the key.

        Args:
            key (hashable): The unique identifier of the set. 
            minhash (datasketch.MinHash): The MinHash of the set. 
        """
        if len(minhash) != self.h:
            raise ValueError("Expecting minhash with length %d, got %d"
                    % (self.h, len(minhash)))
        if key in self.keys:
            raise ValueError("The given key already exists")
        Hs = [self._H(minhash.hashvalues[start:end])
                for start, end in self.hashranges]
        for H in Hs:
            self.keys.insert(key, H)
        for H, hashtable in zip(Hs, self.hashtables):
            hashtable.insert(H, key)

    def query(self, minhash):
        """
        Giving the MinHash of the query set, retrieve 
        the keys that references sets with Jaccard
        similarities greater than the threshold.
        
        Args:
            minhash (datasketch.MinHash): The MinHash of the query set. 

        Returns:
            `list` of keys.
        """
        if len(minhash) != self.h:
            raise ValueError("Expecting minhash with length %d, got %d"
                    % (self.h, len(minhash)))
        candidates = set()
        for (start, end), hashtable in zip(self.hashranges, self.hashtables):
            H = self._H(minhash.hashvalues[start:end])
            for key in hashtable.get(H):
                candidates.add(key)
        return list(candidates)

    def __contains__(self, key):
        """
        Args:
            key (hashable): The unique identifier of a set.

        Returns: 
            bool: True only if the key exists in the index.
        """
        return key in self.keys

    def remove(self, key):
        """
        Remove the key from the index.

        Args:
            key (hashable): The unique identifier of a set.
        """
        if key not in self.keys:
            raise ValueError("The given key does not exist")
        for H, hashtable in zip(self.keys[key], self.hashtables):
            hashtable.remove_val(H, key)
            if not hashtable.get(H):
                hashtable.remove(H)
        self.keys.remove(key)

    def is_empty(self):
        """
        Returns:
            bool: Check if the index is empty.
        """
        return any(t.size() == 0 for t in self.hashtables)

    @staticmethod
    def _H(hs):
        return bytes(hs.byteswap().data)

    def _query_b(self, minhash, b):
        if len(minhash) != self.h:
            raise ValueError("Expecting minhash with length %d, got %d"
                    % (self.h, len(minhash)))
        if b > len(self.hashtables):
            raise ValueError("b must be less or equal to the number of hash tables")
        candidates = set()
        for (start, end), hashtable in zip(self.hashranges[:b], self.hashtables[:b]):
            H = self._H(minhash.hashvalues[start:end])
            if H in hashtable:
                for key in hashtable[H]:
                    candidates.add(key)
        return candidates

    def get_counts(self, **kwargs):
        self.counts_ = [
            hashtable.itemcounts(**kwargs) for hashtable in self.hashtables]
        return self.counts_

    def __getstate__(self):
        state = self.__dict__.copy()
        # Do not persist counts
        state.pop('counts_', None)
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        # self.get_counts()


class DocMinHashLSH(MinHashLSH):
    """
    Driver for MinHashLSH.
    """
    def __init__(self, threshold=0.9, num_perm=128, weights=(0.5,0.5),
                 storage_config={'type':'dict'}, random_seed=1,
                 display_progress=True):
        super(DocMinHashLSH, self).__init__(
            threshold=threshold, num_perm=num_perm,
            weights=weights, storage_config=storage_config)
        self.random_seed = random_seed
        self.display_progress = display_progress
        self.storage_config = storage_config

    def insert_documents(self, *key_documents):
        for key, document in key_documents:
            m = MinHash(num_perm=self.h, seed=self.random_seed)
            for word in document:
                if isinstance(word, str):
                    m.update(word.encode('utf8'))
                elif isinstance(word, tuple):
                    text, multiplicity = word
                    for i in range(multiplicity):
                        token = text + str(i)
                        m.update(token.encode('utf8'))
            self.insert(key.encode('utf8'), m)

    def query_keys(self, *keys):
        keys = [x.encode('utf8') for x in keys]
        to_union = set()
        Hss = self.keys.getmany(*keys)
        for key, Hs in zip(keys, Hss):
            for H, hashtable in zip(Hs, self.hashtables):
                to_union.add(hashtable.get_reference(H))
        r = self.hashtables[0].union_references(*to_union)
        for key in keys:
            r.discard(key)
        l = [x.decode('utf8') for x in r]
        return l

    def get_subset_counts(self, *keys):
        key_set = list(set(x.encode('utf8') for x in keys))
        hashtables = [unordered_storage({'type': 'dict'}) for _ in
                      range(self.b)]
        Hss = self.keys.getmany(*key_set)
        for key, Hs in zip(key_set, Hss):
            for H, hashtable in zip(Hs, hashtables):
                hashtable.insert(H, key)
        return [hashtable.itemcounts() for hashtable in hashtables]

    def get_hashvalues(self, *keys):
        return self.keys.getmany(*[key.encode('utf8') for key in keys])

    def get_status(self):
        return self.keys.status()
