from mongo_factory.mongo_starter import MongoStarter
import pprint

post = [
    {'_id': 1,
        "items": [
            {
                "name": "pens",
                "tags": ["writing", "office", "school", "stationary"],
                "price": 12.00,
                "quantity": 5
            },
            {
                "name": "envelopes",
                "tags": ["stationary", "office"],
                "price": 1.95,
                "quantity": 8
            }
        ]
     },
    {
        '_id': 2,
        "items": [
            {
                "name": "laptop",
                "tags": ["office", "electronics"],
                "price": 800.00,
                "quantity": 1
            },
            {
                "name": "notepad",
                "tags": ["stationary", "school"],
                "price": 14.95,
                "quantity": 3
            }
        ]
    }
]


class Mydb(MongoStarter):
    def __init__(self, db_name, collection_name):
        super().__init__(db_name, collection_name)

    def add_many_post(self, post):
        self._add_many_post(post)

    def add_single_post(self, post):
        self._add_single_post(post)

    def deneme(self):
        pipeline = []
        pipeline.append({'$unwind': '$items'})
        pipeline.append({'$unwind': '$items.tags'})
        pipeline.append(
            {'$group': {'_id': '$items.tags', 'totalAmount': {'$sum': {'$multiply': ['$items.price', '$items.quantity']}}}})
        return list(self.collection.aggregate(pipeline))


mydb = Mydb('unwind', 'my_col')
# mydb.add_many_post(post)
pprint.pprint(mydb.deneme())
