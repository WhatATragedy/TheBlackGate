from pymongo import MongoClient
from random import randint
class MongoInterface():
    def __init__(self, host=None, port=None):
        self.port = port if port is not None else 27017
        self.host = host if host is not None else 'localhost'
        self.client = MongoClient(host=self.host, port=self.port)
    
    def insert_dict(self, data_dict, db_name, collection_name):
        #TODO check if DB exists and collection
        db=self.client[db_name]
        for item in data_dict:
            result = db[collection_name].insert_one(item)
            print('Created {0} of some_amount as {1}'.format(item,result.inserted_id))
        print('finished creating asn')
        return True