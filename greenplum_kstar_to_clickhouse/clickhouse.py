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


def insert_athlete(cl: Client, data):
    cl.insert(
        "mireaDB.athlete",
        data,
        column_names=["name", "age", "sex", "height", "weight", "team_name", "noc_code"]
    )


def insert_event(cl: Client, data):
    cl.insert("mireaDB.event", data, column_names=["name", "sport_name"])


def insert_game(cl: Client, data):
    cl.insert("mireaDB.game", data, column_names=["year", "season", "city", "name"])


def insert_medal(cl: Client, data):
    cl.insert("mireaDB.medal", data, column_names=["name"])


def insert_result(cl: Client, data):
    cl.insert("mireaDB.result", data, column_names=["value"])


def find_in_list(name, list):
    for i in range(len(list)):
        if list[i][1] == name:
            return list[i][0]
    return None


def insert_participation(cl: Client, data):
    athletes = cl.query("SELECT * FROM mireaDB.athlete").result_rows
    games = cl.query("SELECT * FROM mireaDB.game").result_rows
    events = cl.query("SELECT * FROM mireaDB.event").result_rows
    medals = cl.query("SELECT * FROM mireaDB.medal").result_rows
    result = cl.query("SELECT * FROM mireaDB.result").result_rows

    to_insert_nn_nn = []
    to_insert_n_nn = []
    to_insert_nn_n = []

    for d in data:
        a_id = find_in_list(d[0], athletes)
        g_id = find_in_list(d[1], games)
        e_id = find_in_list(d[2], events)
        m_id = None
        r_id = None

        if d[3] is not None:
            m_id = find_in_list(d[3], medals)
        else:
            to_insert_n_nn.append([a_id, g_id, e_id, r_id])
        if d[4] is not None:
            r_id = find_in_list(d[4], result)
        else:
            to_insert_nn_n.append([a_id, g_id, e_id, m_id])

        # if m_id is not None and r_id is not None:
        #     to_insert_nn_nn.append([a_id, g_id, e_id, m_id, r_id])
        to_insert_nn_nn.append([a_id, g_id, e_id, m_id, r_id])

    cl.insert("mireaDB.participation", to_insert_nn_nn,
              column_names=["athlete_id", "game_id", "event_id", "medal_id", "result_id"])

    # cl.insert("mireaDB.participation", to_insert_n_nn,
    #           column_names=["athlete_id", "game_id", "event_id", "result_id"])
    #
    # cl.insert("mireaDB.participation", to_insert_nn_n,
    #           column_names=["athlete_id", "game_id", "event_id", "medal_id"])
