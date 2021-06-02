from enum import unique
import pymongo
import datetime
import pprint
from bson.objectid import ObjectId


class MongoStudy:
    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
        self.mydb = self.client['burak_db']
        self.collection = self.mydb['my_collection']
        self.post = {'id': 1,
                     "author": "Mike",
                     "text": "My first blog post!",
                     "tags": ["mongodb", "python", "pymongo"],
                     "date": datetime.datetime.utcnow().strftime('%m/%d/%Y')}

# collection.insert_one(post)

####### print(mydb.list_collection_names()) returns the collections inside a db    ######

# Getting a Single Document With find_one()

# $$$$$$$ The most basic type of query that can be performed in MongoDB is find_one().
#  This method returns a single document matching a query (or None if there are no matches).
#  It is useful when you know there is only one matching document, or are only interested in the first match.
# Here we use find_one() to get the first document from the posts collection:    $$$$$$$

# my_doc = collection.find_one()

# pprint.pprint(my_doc)

# find_one() also supports querying on specific elements that the resulting document must match.
# To limit our results to a document with author “Mike” we do: ##


class Examples(MongoStudy):
    def __init__(self):
        super().__init__()

    def test_adding_new_document_to_db(self):
        my_doc = self.collection.insert_one(self.post)

    def test_querying_document_from_db(self):
        specific_field_search = {'author': 'Mike'}
        my_doc = self.collection.find_one(specific_field_search)
        pprint.pprint(my_doc)

    def test_querying_non_existing_document_from_db(self):
        non_existing_field_search = {'text': 'My second blog post!'}
        # Returns None since document doesn't exist in mongodb
        my_doc = self.collection.find_one(non_existing_field_search)
        pprint.pprint(my_doc)

    def test_querying_by_object_id_with_bson(self):
        post_id = {'_id': ObjectId('60b73c26053f6946b497150b')}
        my_doc = self.collection.find_one(post_id)
        pprint.pprint(my_doc)
        print(type(my_doc['_id']))

    def test_inserting_many_documents(self):
        posts = [self.post, {'author': 'Burakhan',
                             'text': 'new post!',
                             'title': 'Testing pymongo',
                             'tags': ['test', 'python', 'mongo'],
                             'date':datetime.datetime.utcnow()}]
        self.collection.insert_many(posts)

    def test_querying_more_than_one_document(self):
        matches = self.collection.find()
        for match in matches:
            pprint.pprint(match)

    def test_querying_more_than_one_document_with_param(self):
        query = {'author': 'Burakhan'}
        matches = self.collection.find(query)
        for match in matches:
            pprint.pprint(match)

    def test_count_documents(self):
        query = {'author': 'Burakhan'}
        print(self.collection.count_documents(query))

    def test_complex_query_of_date(self):
        specific_date = datetime.datetime(2022, 2, 1)
        query = {'date': {"$lt": specific_date}}
        matches = self.collection.find(query).sort('author')
        for match in matches:
            pprint.pprint(match)

    def test_indexing(self):
        self.collection.create_index(
            [('id', pymongo.ASCENDING)], unique=True)
        # self.post['id'] = 3
        # self.collection.insert_one({'id': 3, 'author': 'Burak'})
        print(self.collection.index_information())


deneme = Examples()
# deneme.test_adding_new_document_to_db()
# deneme.test_querying_document_from_db()
# deneme.test_querying_non_existing_document_from_db()
# deneme.test_querying_by_object_id_with_bson()
# deneme.test_inserting_many_documents()
# deneme.test_querying_more_than_one_document()
# deneme.test_querying_more_than_one_document_with_param()
# deneme.test_count_documents()
# deneme.test_complex_query_of_date()
deneme.test_indexing()
