from pymongo import MongoClient

from config import mongoUrl, mongo_db_name, mongo_coll_name


def mongo_get_collection():
    try:
        return MongoClient(mongoUrl)[mongo_db_name][mongo_coll_name]
    except Exception as e:
        print('Can`t establish connection to database MONGODB')
