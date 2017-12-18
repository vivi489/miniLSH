import os
from pymongo import MongoClient

def dump_buckets(buckets, path, context=False, mapper=None):
    if not context:
        with open(path, "w") as fout:
            for b in buckets:
                if len(b) > 1:
                    fout.write(" ".join([str(x) for x in b]))
                    fout.write('\n')
    else:
        assert mapper is not None, "invalid document mapper"
        if not os.path.exists(path):
            os.mkdir(path)
        bIdx = 0
        for b in buckets:
            if len(b) > 1:
                with open(os.path.join(path, str(bIdx)+".txt"), "w") as fout:
                    for idx in b:
                        fout.write(str(idx)+"\t"+mapper.get_doc(idx))
                        fout.write('\n')
                bIdx += 1

class DocMapper:
    def __init__(self):
        pass
    
    def get_doc(self, idx):
        return None

class DocFileMapper(DocMapper):
    def __init__(self, path):
        self.lookup = {}
        idx = 0
        with open(self.path, 'r') as f:
            for doc in f:
                self.lookup[idx] = doc
                idx += 1
    
    def get_doc(self, idx):
        return self.lookup[idx].strip()

class DocCollectionMapper(DocMapper):
    def __init__(self, clt, token_attrib):
        self.clt = clt
        self.attrib = token_attrib
    
    def get_doc(self, idx):
        doc = self.clt.find_one({"web_id": idx})
        return doc[self.attrib].strip()


