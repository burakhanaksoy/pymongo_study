import pymongo
import pprint


class Aggregation:
    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
        self.mydb = self.client['examples_2']
        self.collection = self.mydb['my_collection_2']


class Examples(Aggregation):
    def __init__(self):
        super().__init__()

    def add_data(self):
        post = [{
            'student': "Maya",
            'homework': [10, 5, 10],
            'quiz': [10, 8],
            'extraCredit': 0
        },
            {
            'student': "Ryan",
            'homework': [5, 6, 5],
            'quiz': [8, 8],
            'extraCredit': 8
        }]

        self.collection.insert_many(post)

    def test_aggregation_add_fields(self):
        # Adds new fields to documents.
        # $addFields outputs documents that contain all existing fields from the input documents and newly added fields.

        # The $addFields stage is equivalent to a $project stage that explicitly specifies all existing fields
        # in the input documents and adds the new fields.

        pipeline = [
            {
                '$addFields': {
                    'totalHomework': {'$sum': '$homework'},
                    'totalQuiz': {'$sum': '$quiz'}
                }
            },
            {
                '$addFields': {'totalScore': {
                    '$add': ['$totalQuiz', '$totalHomework', '$extraCredit']}
                }
            }
        ]
        pprint.pprint(list(self.collection.aggregate(pipeline)))


examples = Examples()
# examples.add_data()
examples.test_aggregation_add_fields()
