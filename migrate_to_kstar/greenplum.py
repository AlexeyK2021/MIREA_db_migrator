from prom_green_value_connector import event_name


def insert_athlete(gconn, athlete: dict):
    """
    Inserting athlete table in greenplum
    :param gconn: greenplum connection
    :param athlete: {"Name": [name:str, sex:str, age:int, height:int, weight:int, team_name:str, noc_code:str]}
    :return:
    """
    with gconn.cursor() as gcur:
        for k, a in athlete.items():
            if k is None or a[0] is None or a[1] is None or a[2] is None or a[3] is None or a[4] is None or a[
                5] is None:
                continue
            gcur.execute(
                f"INSERT INTO kstar.athlete(name, sex, age, height, weight, team_name, noc_code) VALUES ("
                f"'{k}', '{a[0]}', {a[1]}, {a[2]}, {a[3]}, '{a[4]}', '{a[5]}');"
            )


def insert_event(gconn, event: dict):
    """
    Inserting event table in greenplum
    :param gconn: greenplum connection
    :param event: [name:str, sport_name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for e, s in event.items():
            if e is None or s[0] is None:
                continue
            gcur.execute(f"INSERT INTO kstar.event(name, sport_name) VALUES (" +
                         f"'{e}', '{s[0]}');")


def insert_game(gconn, game: list):
    """
    Inserting game table in greenplum
    :param gconn: greenplum connection
    :param game: [name:str, year:int, season:str, city:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for g in game:
            if g[0] is None or g[1] is None or g[2] is None or g[3] is None:
                continue
            gcur.execute(f"INSERT INTO kstar.game (name, year, season, city) VALUES (" +
                         f"'{g[0]}', {g[1]}, '{g[2]}', '{g[3]}');")


def insert_medal(gconn, medal: list):
    """
    Inserting medal table in greenplum
    :param gconn: greenplum connection
    :param medal: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for m in medal:
            if m is None:
                continue
            gcur.execute(f"INSERT INTO kstar.medal(name) VALUES ('{m}')")


def find_in_list(name, list):
    for i in range(len(list)):
        if list[i][1] == name:
            return list[i][0]
    return None


def insert_participation(gconn, participation: list):
    """
    Inserting participation table in greenplum
    :param gconn: greenplum connection
    :param participation: [athlete_name:str, game_name:str, event_name:str, medal_name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        gcur.execute("SELECT id, name FROM kstar.athlete")
        athletes = gcur.fetchall()
        gcur.execute("SELECT id, name FROM kstar.game")
        games = gcur.fetchall()
        gcur.execute("SELECT id, name FROM kstar.event")
        events = gcur.fetchall()

        for p in participation:
            if p[0] is None or p[1] is None or p[2] is None:
                continue

            a_id = find_in_list(p[0], athletes)
            if a_id is None:
                continue

            g_id = find_in_list(p[1], games)
            if g_id is None:
                continue

            e_id = find_in_list(p[2], events)
            if e_id is None:
                continue

            m_id = None
            if p[3] is not None:
                gcur.execute(f"SELECT id FROM kstar.medal WHERE name='{p[3]}'")
                m_id = gcur.fetchone()

            if m_id is not None:
                gcur.execute(f"INSERT INTO kstar.participation(athlete_id, game_id, event_id, medal_id) VALUES ("
                             f"{a_id}, "
                             f"{g_id}, "
                             f"{e_id}, "
                             f"{m_id[0]}"
                             ")")  # Вставляет только значения medal_id != NULL
            else:
                gcur.execute(f"INSERT INTO kstar.participation(athlete_id, game_id, event_id) VALUES ("
                             f"{a_id}, "
                             f"{g_id}, "
                             f"{e_id}"
                             ")")


def insert_result(gconn, results: list):
    """
    Inserting result table in greenplum
    :param gconn: greenplum connection
    :param results: json[{athlete_name:str, event:str, value:str, year:str}]
    :return:
    """
    with gconn.cursor() as gcur:
        gcur.execute("SELECT id, name FROM kstar.event")
        events_list = gcur.fetchall()
        gcur.execute("SELECT id, name FROM kstar.game")
        games_list = gcur.fetchall()
        # gcur.execute("SELECT id FROM kstar.participation")
        # participation_list = gcur.fetchall()

        for result in results:
            if 'value' not in result.keys():
                continue
            name = "%" + result['athlete_name'].replace(" ", "%").lower() + "%"
            if "'" in name:
                continue

            gcur.execute(f"SELECT id FROM kstar.athlete WHERE LOWER(name) LIKE '{name}'")
            athlete_id = gcur.fetchone()
            if athlete_id is None:
                continue
            else:
                athlete_id = athlete_id[0]

            event_ = event_name[result['event']]
            event_id = find_in_list(event_, events_list)

            year = int(result['year'])
            game_id = find_in_list(f"{year} Summer", games_list)

            gcur.execute(f"INSERT INTO kstar.result(value) VALUES ({int(result['value'])}) RETURNING id")
            res_id = gcur.fetchone()

            gcur.execute(f"UPDATE kstar.participation SET result_id = {res_id[0]} WHERE athlete_id = {athlete_id} AND game_id = {game_id} AND event_id = {event_id}")

def truncate_database(gconn):
    """
    Truncate database
    :param gconn: greenplum connection
    :param database_name: database name to truncate
    :return:
    """
    print(f"Truncate database Kimball Star")
    with gconn.cursor() as gcur:
        gcur.execute(f"TRUNCATE TABLE kstar.result")
        gcur.execute(f"TRUNCATE TABLE kstar.participation")
        gcur.execute(f"TRUNCATE TABLE kstar.athlete")
        gcur.execute(f"TRUNCATE TABLE kstar.game")
        gcur.execute(f"TRUNCATE TABLE kstar.event")
        gcur.execute(f"TRUNCATE TABLE kstar.medal")
