import farmhash, time, pymongo
from collections import defaultdict,  deque
from operator import itemgetter
import numpy as np

def _get_gram_count(doc, n):
    tokens = doc.strip().split()
    if len(tokens) < n: return None
    q = deque(tokens[:n], maxlen=n)
    dict_doc_gram = defaultdict(int)
    dict_doc_gram[" ".join(tokens[:n])] += 1
    for i in range(n, len(tokens)):
        q.append(tokens[i])
        dict_doc_gram[" ".join(list(q))] += 1
    del tokens
    return dict_doc_gram

def _get_vocab_tuple(docIterator, n):
    dict_vocab = defaultdict(lambda: tuple([0, 0]))
    for doc in docIterator.next_doc():
        dict_doc_gram = _get_gram_count(doc, n)
        if dict_doc_gram is None: continue
        for t in dict_doc_gram:
            dict_vocab[t] = (max(dict_vocab[t][0], dict_doc_gram[t]), dict_vocab[t][0] + dict_doc_gram[t])
    tc = sorted([(t, w, c) for t, (w, c) in dict_vocab.items()], key=itemgetter(2), reverse=True)
    #tc = {tc[i][0]: i for i in range(len(tc))}
    return tc

def get_feature_map(dociter, n=2): # data digestion
    vocab_w_c = _get_vocab_tuple(dociter, n)
    fmap = {}
    index = 0
    for t, w, _ in vocab_w_c:
        fmap[t] = (index, index+w)
        index += w
    del vocab_w_c
    return fmap

def get_doc_feature(doc, fmap, n=2):
    retVal = []
    dict_doc_gram = _get_gram_count(doc, n)
    if dict_doc_gram is None: return []
    for g in dict_doc_gram:
        intv = fmap[g]
        retVal.extend(list(range(*intv)))
    retVal.sort()
    return retVal

class DocIterator:
    def __init__(self):
        pass
    
    def next_doc(self, index=False):
        return None


class DocFileIterator(DocIterator):
    def __init__(self, path):
        self.path = path
        self.idx = 0
    
    def next_doc(self, index=False):
        with open(self.path, 'r') as f:
            for doc in f:
                yield (self.idx, doc) if index else doc
                self.idx += 1

class DocCollectionIterator(DocIterator):
    def __init__(self, clt, token_attrib):
        self.clt = clt
        self.attrib = token_attrib
    
    def next_doc(self, index=False):
        for doc in clt.find(snapshot=True):
            yield (doc["web_id"], doc[self.attrib]) if index else doc[self.attrib]


class LSH:
    def __init__(self, col_size, salts, b, n=2):
        self.col_size = col_size
        self.salts = salts # len(salts) == r
        self.b = b
    
    def minhash(self, doc): #return a list of signature
        assert len(doc) > 0, "empty signature found; hashing aborted"
        retVal = []
        perms = [lambda x: farmhash.hash64(str(x+salt))%self.col_size for salt in self.salts]
        for perm in perms:
            retVal.append(doc[np.argmin([perm(x) for x in doc])])
        return retVal
    
    def digest(self, docs):
        retVal = []
        for i in range(self.b):
            print("iteration %d"%i)
            id2buckets = defaultdict(list)
            for idx, doc in docs:
                sig = tuple(self.minhash(doc))
                id2buckets[sig].append(idx)
            retVal.extend(list(id2buckets.values()))
        return retVal
    
    def plot(path):
        r = len(self.salts); b = self.b
        s = np.arange(0, 1.01, 0.01)
        plt.scatter(s, 1-(1-s**r)**b)


class FHash:
    def __init__(self, size, salt):
        self.size = size
        self.salt = salt
    
    def transform(self, doc):
        retVal = np.array([0] * self.size)
        for x in doc:
            idx = (x+self.salt)%self.size
            delta = 1 if farmhash.hash64(str(x+self.salt))%2==0 else -1
            retVal[idx] += delta
        return retVal
    
    def digest(self, docs):
        return [(idx, self.transform(doc)) for idx, doc in docs]



