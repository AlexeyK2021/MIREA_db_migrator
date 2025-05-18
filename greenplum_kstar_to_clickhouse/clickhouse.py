import clickhouse_connect
from clickhouse_connect.driver import Client

from config import clickhouse_ip, clickhouse_user, clickhouse_password, clickhouse_db


def get_client():
    return clickhouse_connect.get_client(
        host=clickhouse_ip,
        username=clickhouse_user,
        password=clickhouse_password,
        database=clickhouse_db
    )


def insert_all(cl: Client, data):
    # query = "INSERT INTO mireaDB.athlete VALUES"
    # for a in athlete:
    #     query += f"(generateUUIDv4(), '{a[1]}', {a[2]}, {a[3]}, {a[4]}, '{a[5]}', '{a[6]}'), "
    # print(query)
    cl.insert("mireaDB.athlete_events", data,
              column_names=["name", "sex", "age", "height", "weight", "team", "noc",
                            "games", "year", "season", "city", "sport", "event", "medal", "result"])
    # cl.execute("INSERT INTO mireaDB.athlete VALUES ("
    #            "generateUUIDv4()"
    #            f"{athlete[:][1]}"
    #            ")")
