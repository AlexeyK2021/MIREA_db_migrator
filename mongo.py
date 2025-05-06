from pymongo import MongoClient

from config import mongoUrl, mongo_db_name, mongo_coll_name


def mongo_get_collection():
    try:
        return MongoClient(mongoUrl)[mongo_db_name][mongo_coll_name]
    except Exception as e:
        print('Can`t establish connection to database MONGODB')


def mongo_get_column(mcoll, column_name) -> list:
    """
    Selects distinct values from column_name
    :param mcoll: mongo collection
    :param column_name: column to select values
    :return: list of distinct values
    """
    return list(set(mcoll.distinct(column_name)))


# {'_id': ObjectId('67f144e60e6d0557ea0f668c'), 'ID': 76129, 'Name': 'Tanya Lazarova Maslarska', 'Sex': 'F', 'Age': 16, 'Height': 164, 'Weight': 45, 'Team': 'Bulgaria', 'NOC': 'BUL', 'Games': '1992 Summer', 'Year': 1992, 'Season': 'Summer', 'City': 'Barcelona', 'Sport': 'Gymnastics', 'Event': 'Gymnastics Womens Individual All-Around', 'Medal': 'NA'}
def mongo_get_columns(mcoll, key: str, columns_name: list) -> dict:
    data = dict()
    # for row in mcoll.distinct(columns_name):
    #     d = list()
    #     for col in columns_name:
    #         d.append(row[col])
    #     data.append(d)
    for row in mcoll.find():
        el = list()
        for col in row:
            if col in columns_name:
                el.append(row[col])
        data[row[key]] = el
    return data
