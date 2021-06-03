import pymongo


class MongoStarter:
    def __init__(self, db_name, collection_name):
        self.client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
        self.mydb = self.client[db_name]
        self.collection = self.mydb[collection_name]

    def _add_many_post(self, post):
        self.collection.insert_many(post)

    def _add_single_post(self, post):
        self.collection.insert_one(post)
