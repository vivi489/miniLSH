import farmhash, time, pymongo
from collections import defaultdict, deque
from operator import itemgetter
import numpy as np

from miniLSH.hashutils import *
from miniLSH.fileutils import *
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
db = client['query_focus']
clt = db["syukatsu"]


dociter = DocCollectionIterator(clt, "entry")
fm = get_feature_map(dociter, n=2)
col_size = 0
for k, v in fm.items():
    col_size = max(col_size, v[1])

np.random.seed(int(time.time()))
salts = (np.random.rand(10)*10000).astype(np.int32)
lsh = LSH(col_size, salts, 2)
dociter = DocCollectionIterator(clt, "entry")
docs = [(idx, get_doc_feature(doc, fm, n=2)) for idx, doc in dociter.next_doc(index=True)]
docs = [(idx, doc) for idx, doc in docs if len(doc)>0]

buckets = lsh.digest(docs)

dump_buckets(buckets, "buckets", context=True, mapper=DocCollectionMapper(clt, "entry"))
