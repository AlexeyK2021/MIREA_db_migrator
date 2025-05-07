from prom_green_value_connector import event_name


def insert_sex(gconn, sex: list):
    """
    Inserting sex table in greenplum
    :param gconn: greenplum connection
    :param sex: [sex:str]
    :return: nothing
    """
    with gconn.cursor() as gcur:
        for s in sex:
            if s is None:
                continue
            gcur.execute(f"INSERT INTO ksnow.sex(value) VALUES ('{s}')")


def insert_noc(gconn, noc: list):
    with gconn.cursor() as gcur:
        for n in noc:
            if n is None:
                continue
            gcur.execute(f"INSERT INTO ksnow.noc(code) VALUES ('{n}')")


def insert_team(gconn, team: list):
    """
    Inserting team table in greenplum
    :param gconn: greenplum connection
    :param team: [(name:str, noc_code:str)]
    :return:
    """
    with gconn.cursor() as gcur:
        for t in team:
            if t[0] is None or t[1] is None:
                continue
            gcur.execute(f"SELECT id FROM ksnow.noc WHERE code = '{t[1]}'")
            noc_id = gcur.fetchone()
            if noc_id is None:
                continue
            gcur.execute(f"INSERT INTO ksnow.team(name, noc_id) VALUES ('{t[0]}', {noc_id[0]})")


def insert_athlete(gconn, athlete: list):
    """
    Inserting athlete table in greenplum
    :param gconn: greenplum connection
    :param athlete: [(name:str, sex:str, age:int, height:int, weight:int, team:str)]
    :return:
    """
    with gconn.cursor() as gcur:
        for a in athlete:
            if a[0] is None or a[1] is None or a[2] is None or a[3] is None or a[4] is None or a[5] is None:
                continue
            gcur.execute(
                f"INSERT INTO ksnow.athlete(name, sex_id, age, height, weight, team_id) VALUES (" +
                f"'{a[0]}'," +
                f"(SELECT id FROM ksnow.sex WHERE value='{a[1]}')," +
                f"{a[2]}, {a[3]}, {a[4]}," +
                f"(SELECT id FROM ksnow.team WHERE name='{a[5]}')" +
                ")")


def insert_city(gconn, city: list):
    """
    Inserting city table in greenplum
    :param gconn: greenplum connection
    :param city: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for c in city:
            if c is None:
                continue
            gcur.execute(f"INSERT INTO ksnow.city(name) VALUES ('{c}')")


def insert_season(gconn, season: list):
    """
    Inserting season table in greenplum
    :param gconn: greenplum connection
    :param season: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for s in season:
            if s is None:
                continue
            gcur.execute(f"INSERT INTO ksnow.season(name) VALUES ('{s}')")


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
            gcur.execute(f"INSERT INTO ksnow.game (name, year, season_id, city_id) VALUES (" +
                         f"'{g[0]}', {g[1]}," +
                         f"(SELECT id FROM ksnow.season WHERE name='{g[2]}')," +
                         f"(SELECT id FROM ksnow.city WHERE name='{g[3]}')" +
                         ")")


def insert_sport(gconn, sport: list):
    """
    Inserting sport table in greenplum
    :param gconn: greenplum connection
    :param sport: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for s in sport:
            if s is None:
                continue
            gcur.execute(f"INSERT INTO ksnow.sport(name) VALUES ('{s}')")


def insert_event(gconn, event: list):
    """
    Inserting event table in greenplum
    :param gconn: greenplum connection
    :param event: [name:str, sport:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for e in event:
            if e[0] is None or e[1] is None:
                continue
            gcur.execute(f"SELECT id FROM ksnow.sport WHERE name='{e[1]}'")
            sport_id = gcur.fetchone()
            if sport_id is None:
                continue
            gcur.execute(f"INSERT INTO ksnow.event(name, sport_id) VALUES (" +
                         f"'{e[0]}', {sport_id[0]})")


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
            gcur.execute(f"INSERT INTO ksnow.medal(name) VALUES ('{m}')")


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
        gcur.execute("SELECT id, name FROM ksnow.athlete")
        athletes = gcur.fetchall()
        gcur.execute("SELECT id, name FROM ksnow.game")
        games = gcur.fetchall()
        gcur.execute("SELECT id, name FROM ksnow.event")
        events = gcur.fetchall()

        for p in participation:
            if p[0] is None or p[1] is None or p[2] is None:
                continue
            # gcur.execute(f"SELECT id FROM inmon.athlete WHERE name='{p[0]}'")
            # a_id = gcur.fetchone()
            a_id = find_in_list(p[0], athletes)
            if a_id is None:
                continue
            # gcur.execute(f"SELECT id FROM inmon.game WHERE name='{p[1]}'")
            # g_id = gcur.fetchone()
            g_id = find_in_list(p[1], games)
            if g_id is None:
                continue
            # gcur.execute(f"SELECT id FROM inmon.event WHERE name='{p[2]}'")
            # e_id = gcur.fetchone()
            e_id = find_in_list(p[2], events)
            if e_id is None:
                continue

            m_id = None
            if p[3] is not None:
                gcur.execute(f"SELECT id FROM ksnow.medal WHERE name='{p[3]}'")
                m_id = gcur.fetchone()
            if m_id is not None:
                gcur.execute(f"INSERT INTO ksnow.participation(athlete_id, game_id, event_id, medal_id) VALUES ("
                             f"{a_id}, "
                             f"{g_id}, "
                             f"{e_id}, "
                             f"{m_id[0]}"
                             ")")  # Вставляет только значения medal_id != NULL
            else:
                gcur.execute(f"INSERT INTO ksnow.participation(athlete_id, game_id, event_id) VALUES ("
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
        part_id = 0
        athlete_id = 0
        event_id = 0
        game_id = 0

        for result in results:
            name = "%" + result['athlete_name'].replace(" ", "%").lower() + "%"
            if "'" in name:
                continue

            gcur.execute(f"SELECT id FROM ksnow.athlete WHERE LOWER(name) LIKE '{name}'")
            athlete_id = gcur.fetchone()
            if athlete_id is None:
                continue
            else:
                athlete_id = athlete_id[0]

            event_ = event_name[result['event']]
            gcur.execute(f"SELECT id FROM ksnow.event WHERE name LIKE '{event_}'")
            event_id = gcur.fetchone()
            if event_id is None:
                continue
            else:
                event_id = event_id[0]

            year = int(result['year'])
            gcur.execute(f"SELECT id FROM ksnow.game WHERE name = '{year} Summer'")
            game_id = gcur.fetchone()
            if game_id is None:
                continue
            else:
                game_id = game_id[0]

            if "value" not in result.keys():
                continue
            value = float(result['value'])
            gcur.execute(f"INSERT INTO ksnow.result(value) VALUES ({value}) RETURNING id")
            res_id = gcur.fetchone()[0]
            gcur.execute(f"UPDATE ksnow.participation SET result_id = {res_id} WHERE athlete_id = {athlete_id} AND game_id = {game_id} AND event_id = {event_id}")  # ошибка из-за того что нету в participation некоторых id


def truncate_database(gconn):
    """
    Truncate database
    :param gconn: greenplum connection
    :param database_name: database name to truncate
    :return:
    """
    print(f"Truncate database Inmon")
    with gconn.cursor() as gcur:
        gcur.execute(f"TRUNCATE TABLE ksnow.result")
        gcur.execute(f"TRUNCATE TABLE ksnow.participation")
        gcur.execute(f"TRUNCATE TABLE ksnow.athlete")
        gcur.execute(f"TRUNCATE TABLE ksnow.game")
        gcur.execute(f"TRUNCATE TABLE ksnow.event")
        gcur.execute(f"TRUNCATE TABLE ksnow.medal")
        gcur.execute(f"TRUNCATE TABLE ksnow.sport")
        gcur.execute(f"TRUNCATE TABLE ksnow.season")
        gcur.execute(f"TRUNCATE TABLE ksnow.sport")
        gcur.execute(f"TRUNCATE TABLE ksnow.city")
        gcur.execute(f"TRUNCATE TABLE ksnow.sex")
        gcur.execute(f"TRUNCATE TABLE ksnow.noc")
        gcur.execute(f"TRUNCATE TABLE ksnow.team")
