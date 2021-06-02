import pymongo
import pprint
import datetime
from bson.objectid import ObjectId


class Data:
    def __init__(self, is_cute, tags):
        self.is_cute = is_cute
        self.tags = tags
        self.time = datetime.datetime.now()


class Aggregation:
    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
        self.mydb = self.client['animals']
        self.collection = self.mydb['animals_collection']


class Examples(Aggregation):
    def __init__(self):
        super().__init__()

    def add_data(self):
        import random
        animals = ['Elephant', 'Monkey', 'Koala',
                   'Zebra', 'Lion', 'Cat', 'Dog', 'Leopard']
        is_cute = [True, False]
        data_list = []

        for _ in range(1, 11):
            # x = random.randint(0, 7)
            # y = random.randint(x, 7)
            tags = animals[random.randint(0, 7)]
            data = Data(random.choice(is_cute), tags)
            data_list.append(data.__dict__)

        self.collection.insert_many(data_list)

    def test_size_operator(self):
        matches = self.collection.find({"tags": {"$size": 2}})

        for match in matches:
            print(match)

    def test_aggregation_unwind(self):
        # Deconstructs an array field from the input documents to output a document for each element.
        # Each output document is the input document with the value of the array field replaced by the element.
        pipeline = [
            {"$unwind": "$tags"}
        ]
        pprint.pprint(list(self.collection.aggregate(pipeline)))

        # Same thing as pipeline = [
        #       '$unwind':{
        #           'path':'$tags'
        # }
        # ]

        # complex_pipeline = [{
        #     '$unwind': {
        #         'path': '$tags',
        #         'includeArrayIndex': 'arrayIndex'
        #     }
        # }]
        # pprint.pprint(list(self.collection.aggregate(complex_pipeline)))

    def test_aggregation_unwind_two(self):
        pipeline = [
            {'$unwind': {
                'path': '$tags'
            }},
            # First stage

            {'$group': {
                '_id': '$tags',
                'count': {'$sum': 1}
            }},
            # Second stage

            {'$sort': {
                'count': -1
                # count : -1 DESCENDING
                # count: 1 ASCENDING
            }}

        ]
        pprint.pprint(list(self.collection.aggregate(pipeline)))

    def test_aggregation_group(self):
        # Groups input documents by the specified _id expression and for each distinct grouping, outputs a document.
        # The _id field of each output document contains the unique group by value.

        # Generally used with other pipeline stages

        pipeline = list()
        pipeline.append({'$sort': {'time': 1}})
        pipeline.append({'$group': {'_id': '$tags', 'count': {'$sum': 1}}})
        pipeline.append({'$sort': {'count': -1}})

        pipeline_result = self.collection.aggregate(pipeline)
        pprint.pprint(list(pipeline_result))

        # groups animals by kind and prints out

    def test_group_documents_by_x_values(self):
        pipeline = [

            {'$unwind': {
                'path': '$tags',
                'preserveNullAndEmptyArrays': True
            }},
            {
                '$group': {
                    "_id": '$tags',

                    'tags': {'$push': '$time'}
                }
            },
            {'$sort': {'_id': 1}}

        ]
        pipeline_result = self.collection.aggregate(pipeline)
        pprint.pprint(list(pipeline_result))

    def test_aggregation_match(self):
        # Filters the documents to pass only the documents that match the specified condition(s) to the next pipeline stage.

        # Place the $match as early in the aggregation pipeline as possible.
        # Because $match limits the total number of documents in the aggregation pipeline,
        # earlier $match operations minimize the amount of processing down the pipe.

        pipeline = list()
        pipeline.append({'$match': {'tags': 'Zebra'}})
        pipeline.append({'$group': {'_id': None, 'count': {'$sum': 1}}})

        result = self.collection.aggregate(pipeline)
        pprint.pprint(list(result))

    def test_aggregation_project(self):
        # Passes along the documents with the requested fields to the next stage in the pipeline.
        # The specified fields can be existing fields from the input documents or newly computed fields.

        pipeline = list()
        pipeline.append(
            {'$project': {"_id": 1, 'tags': {'$cond': {'if': {'$eq': [[], '$tags']}, 'then': "$$REMOVE", 'else': '$tags'}}}})

        result = self.collection.aggregate(pipeline)
        pprint.pprint(list(result))

    def test_aggregation_limit(self):
        # Limits the number of documents passed to the next stage in the pipeline.
        # $limit takes a positive integer that specifies the maximum number of documents to pass along.

        pipeline = list()
        pipeline.append({'$sort': {'time': 1}})
        pipeline.append({'$limit': 5})
        pipeline.append(({'$addFields': {'newBorn': True}}))
        result = self.collection.aggregate(pipeline)
        pprint.pprint(list(result))


examples = Examples()
# examples.add_data()
# examples.test_size_operator()
# examples.test_aggregation_unwind()
# examples.test_aggregation_unwind_two()
# examples.test_aggregation_group()
# examples.test_group_documents_by_x_values()
# examples.test_aggregation_match()
# examples.test_aggregation_project()
# examples.test_aggregation_limit()
