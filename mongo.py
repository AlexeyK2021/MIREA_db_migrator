from pymongo import MongoClient

from config import mongoUrl, mongo_db_name, mongo_coll_name


def mongo_get_collection():
    try:
        return MongoClient(mongoUrl)[mongo_db_name][mongo_coll_name]
    except Exception as e:
        print('Can`t establish connection to database MONGODB')


def mongo_get_coll(mcoll, column_name) -> set:
    """
    Selects distinct values from column_name
    :param mcoll: mongo collection
    :param column_name: column to select values
    :return: list of distinct values
    """
    return set(mcoll.distinct(column_name))


def mongo_get_columns(mcoll, columns_name: list) -> list:
    data = list(list())
    for row in mcoll.distinct(columns_name):
        d = list()
        for col in columns_name:
            d.append(row[col])
        data.append(d)
    return data
