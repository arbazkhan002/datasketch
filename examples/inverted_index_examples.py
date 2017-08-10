'''
Some examples for inverted index.
'''

from datasketch import InvertedIndex

set1 = set(['minhash', 'is', 'a', 'probabilistic', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'datasets'])
set2 = set(['minhash', 'is', 'a', 'probability', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'documents'])
set3 = set(['minhash', 'is', 'probability', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'documents'])


def eg1():
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


if __name__ == "__main__":
    eg1()
