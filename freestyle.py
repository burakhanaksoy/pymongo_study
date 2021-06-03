import mongo_starter

post = [{'product': 'toothbrush', 'total': 4.75, 'customer': 'Mike'},
        {'product': 'guitar', 'total': 199.99, 'customer': 'Tom'},
        {'product': 'milk', 'total': 11.33, 'customer': 'Mike'},
        {'product': 'pizza', 'total': 8.50, 'customer': 'Karen'},
        {'product': 'toothbrush', 'total': 4.75, 'customer': 'Karen'},
        {'product': 'pizza', 'total': 4.75, 'customer': 'Dave'},
        {'product': 'toothbrush', 'total': 4.75, 'customer': 'Mike'}]


class Mydb(mongo_starter.MongoStarter):
    def __init__(self, db_name, collection_name):
        super().__init__(db_name, collection_name)

    def add_many_post(self, post):
        self._add_many_post(post)

    def add_single_post(self, post):
        self._add_single_post(post)

    #  find out how many `items` were sold
    def find_items_sold(self, item):
        return self.collection.count({'product': item})

    # Find the list of all products sold
    def find_list_of_all_products_sold(self):
        return self.collection.distinct('product')

    # Find the total money spent by each customer
    def find_money_spent_by_each_customer(self):
        pipeline = []
        pipeline.append(
            {'$group': {'_id': '$customer', 'total': {'$sum': '$total'}}})
        pipeline.append({'$sort': {'total': -1}})

        return list(self.collection.aggregate(pipeline))


mydb = Mydb('freestyle', 'my_col')
# mydb.add_many_post(post)
# print(mydb.find_items_sold('toothbrush'))
# print(mydb.find_list_of_all_products_sold())
# print(mydb.find_money_spent_by_each_customer())
