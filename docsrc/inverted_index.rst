.. _inverted_index:

Inverted Index
==============

An inverted index is one of the oldest and simplest ways of storing and
querying data that takes the form of
`sets <https://en.wikipedia.org/wiki/Set_(mathematics)>`__. For more
details on the inverted index, see
`Manning <https://nlp.stanford.edu/IR-book/pdf/irbookonlinereading.pdf>`_ page
6.

The inverted index is provided in this package primarily for comparison with
other more sophisticated indexes.

An inverted index consists of a collection of *documents*. For each element
present in one or more documents, there is also a postings list. The postings
list is a list of documents in which that element appears.

.. code:: python
        
        from datasketch import InvertedIndex

        set1 = set(['minhash', 'is', 'a', 'probabilistic', 'data', 'structure', 'for',
                    'estimating', 'the', 'similarity', 'between', 'datasets'])
        set2 = set(['minhash', 'is', 'a', 'probability', 'data', 'structure', 'for',
                    'estimating', 'the', 'similarity', 'between', 'documents'])
        set3 = set(['minhash', 'is', 'probability', 'data', 'structure', 'for',
                    'estimating', 'the', 'similarity', 'between', 'documents'])

        # Create LSH index
        index = InvertedIndex()
        for d in set1:
            index.insert("m1", d)
        for d in set2:
            index.insert("m2", d)
        for d in set3:
            index.insert("m3", d)

        result = index.query("documents")
        print("Documents containing the term 'documents'", result)


Inverted indexes at scale
-------------------------
The inverted index supports a Redis backend for querying large datasets as part of
a production environment. For more details, see :ref:`minhash_lsh_scale`.
