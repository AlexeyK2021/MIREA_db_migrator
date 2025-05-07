from prom_green_value_connector import event_name


def insert_event(gconn, event: list):
    """
    Inserting event table in greenplum
    :param gconn: greenplum connection
    :param event: [name:str, sport_name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for e in event:
            if e[0] is None or e[1] is None:
                continue
            gcur.execute(f"INSERT INTO datavault.hub_event(name, sport) VALUES (" +
                         f"'{e[0]}', '{e[1]}');")


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
            gcur.execute(f"INSERT INTO datavault.hub_medal(name) VALUES ('{m}')")


def insert_hub_game(gconn, game: list):
    """
    Inserting game table in greenplum
    :param gconn: greenplum connection
    :param game: [name:str, year:int]
    :return:
    """
    with gconn.cursor() as gcur:
        for g in game:
            if g[0] is None or g[1] is None:
                continue
            gcur.execute(f"INSERT INTO datavault.hub_game (name, year) VALUES (" +
                         f"'{g[0]}', {g[1]});")


def insert_sat_game(gconn, game: dict):
    """
    Inserting game table in greenplum
    :param gconn: greenplum connection
    :param game: [name:str, year:int]
    :return:
    """
    with gconn.cursor() as gcur:
        for g, v in game.items():
            if g is None or v[0] is None or v[1] is None:
                continue
            gcur.execute(f"INSERT INTO datavault.sat_game (game_id, season, city) OVERRIDING SYSTEM VALUE VALUES ("
                         f"(SELECT id FROM datavault.hub_game WHERE name = '{g}'),"
                         f"'{v[0]}', '{v[1]}');")


def insert_hub_athlete(gconn, athlete: list):
    """
    Inserting athlete table in greenplum
    :param gconn: greenplum connection
    :param athlete: [[name:str, sex:str]]
    :return:
    """
    with gconn.cursor() as gcur:
        for a in athlete:
            if a[0] is None or a[1] is None:
                continue
            gcur.execute(
                f"INSERT INTO datavault.hub_athlete(name, sex) VALUES ("
                f"'{a[0]}', '{a[1]}');"
            )


def insert_sat_athlete(gconn, athlete: dict):
    """
    Inserting athlete table in greenplum
    :param gconn: greenplum connection
    :param athlete: {"Name": [age:int, height:int, weight:int, team:str, noc:str]]
    :return:
    """
    with gconn.cursor() as gcur:
        for a, v in athlete.items():
            if a is None or v[0] is None or v[1] is None or v[2] is None or v[3] is None or v[4] is None:
                continue
            gcur.execute(f"SELECT id FROM datavault.hub_athlete WHERE name = '{a}'")
            a_id = gcur.fetchone()
            if a_id is None:
                continue
            gcur.execute(
                f"INSERT INTO datavault.sat_athlete(athlete_id, age, height, weight, team, noc) OVERRIDING SYSTEM VALUE VALUES ("
                f"{a_id[0]},{v[0]}, {v[1]}, {v[2]}, '{v[3]}', '{v[4]}');"
            )


def find_in_list(name, list):
    for i in range(len(list)):
        if list[i][1] == name:
            return list[i][0]
    return None


def insert_link_participation(gconn, participation: list):
    """
    Inserting participation table in greenplum
    :param gconn: greenplum connection
    :param participation: [athlete_name:str, game_name:str, event_name:str, medal_name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        gcur.execute("SELECT id, name FROM datavault.hub_athlete")
        athletes = gcur.fetchall()
        gcur.execute("SELECT id, name FROM datavault.hub_game")
        games = gcur.fetchall()
        gcur.execute("SELECT id, name FROM datavault.hub_event")
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
                gcur.execute(f"SELECT id FROM datavault.hub_medal WHERE name='{p[3]}'")
                m_id = gcur.fetchone()

            if m_id is not None:
                gcur.execute(f"INSERT INTO datavault.link_participation(athlete_id, game_id, event_id, medal_id) VALUES ("
                             f"{a_id}, "
                             f"{g_id}, "
                             f"{e_id}, "
                             f"{m_id[0]}"
                             ")")  # Вставляет только значения medal_id != NULL
            else:
                gcur.execute(f"INSERT INTO datavault.link_participation(athlete_id, game_id, event_id) VALUES ("
                             f"{a_id}, "
                             f"{g_id}, "
                             f"{e_id}"
                             ")")


def insert_hub_result(gconn, results: list):
    """
    Inserting result table in greenplum
    :param gconn: greenplum connection
    :param results: json[{athlete_name:str, event:str, value:str, year:str}]
    :return:
    """
    with gconn.cursor() as gcur:
        gcur.execute("SELECT id, name FROM datavault.hub_event")
        events_list = gcur.fetchall()
        gcur.execute("SELECT id, name FROM datavault.hub_game")
        games_list = gcur.fetchall()
        # gcur.execute("SELECT id FROM kstar.participation")
        # participation_list = gcur.fetchall()

        for result in results:
            if 'value' not in result.keys():
                continue
            name = "%" + result['athlete_name'].replace(" ", "%").lower() + "%"
            if "'" in name:
                continue

            gcur.execute(f"SELECT id FROM datavault.hub_athlete WHERE LOWER(name) LIKE '{name}'")
            athlete_id = gcur.fetchone()
            if athlete_id is None:
                continue
            else:
                athlete_id = athlete_id[0]

            event_ = event_name[result['event']]
            event_id = find_in_list(event_, events_list)

            year = int(result['year'])
            game_id = find_in_list(f"{year} Summer", games_list)

            gcur.execute(f"INSERT INTO datavault.hub_result(value) VALUES ({int(result['value'])}) RETURNING id")
            res_id = gcur.fetchone()

            gcur.execute(f"UPDATE datavault.link_participation SET result_id = {res_id[0]} WHERE athlete_id = {athlete_id} AND game_id = {game_id} AND event_id = {event_id}")
