'''
Some examples for MinHash
'''

from datasketch.minhash import MinHash, MinHashGenerator

data1 = ['minhash', 'is', 'a', 'probabilistic', 'data', 'structure', 'for',
        'estimating', 'the', 'similarity', 'between', 'datasets']
data2 = ['minhash', 'is', 'a', 'probability', 'data', 'structure', 'for',
        'estimating', 'the', 'similarity', 'between', 'documents']

def eg1():
    m1 = MinHash()
    m2 = MinHash()
    for d in data1:
        m1.update(d.encode('utf8'))
    for d in data2:
        m2.update(d.encode('utf8'))
    print("Estimated Jaccard for data1 and data2 is", m1.jaccard(m2))

    s1 = set(data1)
    s2 = set(data2)
    actual_jaccard = float(len(s1.intersection(s2))) /\
            float(len(s1.union(s2)))
    print("Actual Jaccard for data1 and data2 is", actual_jaccard)


def eg2():
    g = MinHashGenerator()
    m1 = g.create(data1)
    m2 = g.create(data2)
    print("Estimated Jaccard for data1 and data2 is", m1.jaccard(m2))

if __name__ == "__main__":
    eg1()
    eg2()
