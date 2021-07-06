# coding:utf-8

# 2020/12/14 上午10:09
"""
DESC:
"""

# mongodb database
from pymongo import MongoClient


class Database(object):
    def __init__(self, address, port, database):
        self.conn = MongoClient(host=address, port=port)
        self.db = self.conn[database]

    def get_state(self):
        return self.conn is not None and self.db is not None

    def insert_one(self, collection, data):
        if self.get_state():
            ret = self.db[collection].insert_one(data)
            return ret.inserted_id
        else:
            return ""

    def insert_many(self, collection, data):
        if self.get_state():
            ret = self.db[collection].insert_many(data)
            return ret.inserted_ids
        else:
            return ""

    def update(self, collection, data):
        # data format:
        # {key:[old_data,new_data]}
        data_filter = {}
        data_revised = {}
        for key in data.keys():
            data_filter[key] = data[key][0]
            data_revised[key] = data[key][1]
        if self.get_state():
            return self.db[collection].update_many(data_filter, {"$set": data_revised}).modified_count
        return 0

    def find(self, col, condition, column=None):
        if self.get_state():
            if column is None:
                return self.db[col].find(condition)
            else:
                return self.db[col].find(condition, column)
        else:
            return None

    def find_one(self, col, condition, column=None):
        if self.get_state():
            if column is None:
                return self.db[col].find_one(condition)
            else:
                return self.db[col].find_one(condition, column)
        else:
            return None

    def find_all_limit(self, collection, start, end):
        if self.get_state():
            return self.db[collection].find().limit(end - start).skip(start)
        else:
            return ""

    def delete(self, col, condition):
        if self.get_state():
            return self.db[col].delete_many(filter=condition).deleted_count
        return 0


if __name__ == '__main__':
    # unit test
    import time
    import random

    db = Database("127.0.0.1", 27017, "test")
    # db = Database("192.168.20.112", 27017, "test")
    print(db.get_state())
    print(db.delete("ut", {}))
    print(time.time())
    start_time = int(time.time() * 1e6)
    for i in range(100):
        t = int(time.time() * 1e6)
        db.insert_one("ut", {"username": str(t),
                             "timestamp": t,
                             "password": "aaaa",
                             "telephone": str(random.random() * 1000000)})
    print("deleted count: ", db.delete("ut", {"timestamp": {"$gt": start_time + 500}}))
    print(db.find("ut", {}).count())
    print(db.update("ut", {"password": ["aaaa", "bbbb"]}))
    print(db.find("ut", {}, {"password": 1, "username": 1}).count())
