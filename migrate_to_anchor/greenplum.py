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
            gcur.execute("INSERT INTO anchor.sex DEFAULT VALUES RETURNING id")
            sex_id = gcur.fetchone()[0]
            gcur.execute(f"INSERT INTO anchor.sex_value(sex_id, value) VALUES ({sex_id}, '{s}');")


def insert_noc(gconn, noc: list):
    """
        Inserting noc table in greenplum
        :param gconn: greenplum connection
        :param noc: [noc:str]
        :return:
        """
    with gconn.cursor() as gcur:
        for n in noc:
            if n is None:
                continue
            gcur.execute("INSERT INTO anchor.noc DEFAULT VALUES RETURNING id")
            noc_id = gcur.fetchone()[0]
            gcur.execute(f"INSERT INTO anchor.noc_code(noc_id, code) VALUES ({noc_id}, '{n}');")


def insert_team(gconn, team: dict):
    """
    Inserting team table in greenplum
    :param gconn: greenplum connection
    :param team: {Team:[Noc:str]}
    :return:
    """
    with gconn.cursor() as gcur:
        for t, v in team.items():
            if t is None or v is None:
                continue

            gcur.execute("INSERT INTO anchor.noc DEFAULT VALUES RETURNING id")
            noc_id = gcur.fetchone()[0]

            gcur.execute("INSERT INTO anchor.team DEFAULT VALUES RETURNING id")
            team_id = gcur.fetchone()[0]

            gcur.execute(f"INSERT INTO anchor.team_noc(team_id, noc_id) VALUES ({team_id}, {noc_id});")

            gcur.execute(f"INSERT INTO anchor.noc_code(noc_id, code) VALUES ({noc_id}, '{v[0]}');")
            gcur.execute(f"INSERT INTO anchor.team_name(team_id, value) VALUES ({team_id}, '{t}');")


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

            gcur.execute("INSERT INTO anchor.athlete DEFAULT VALUES RETURNING id;")
            athlete_id = gcur.fetchone()[0]

            gcur.execute(
                f"SELECT t.id FROM anchor.team AS t JOIN anchor.team_name AS tm ON tm.team_id = t.id WHERE tm.value = '{a[5]}';")
            team_id = gcur.fetchone()[0]

            gcur.execute(
                f"SELECT id FROM anchor.sex AS s JOIN anchor.sex_value AS sv on sv.sex_id = s.id WHERE sv.value = '{a[1]}';")
            sex_id = gcur.fetchone()[0]

            gcur.execute(f"INSERT INTO anchor.athlete_sex(athlete_id, sex_id) VALUES ({athlete_id}, {sex_id});")
            gcur.execute(f"INSERT INTO anchor.athlete_team(athlete_id, team_id) VALUES ({athlete_id}, {team_id});")

            gcur.execute(f"INSERT INTO anchor.athlete_name(athlete_id, value) VALUES ({athlete_id}, '{a[0]}');")
            gcur.execute(f"INSERT INTO anchor.athlete_age(athlete_id, value) VALUES ({athlete_id}, {a[2]});")
            gcur.execute(f"INSERT INTO anchor.athlete_height(athlete_id, value) VALUES ({athlete_id}, {a[3]})")
            gcur.execute(f"INSERT INTO anchor.athlete_weight(athlete_id, value) VALUES ({athlete_id}, {a[4]})")


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

            gcur.execute(f"INSERT INTO anchor.event DEFAULT VALUES RETURNING id;")
            event_id = gcur.fetchone()[0]

            gcur.execute(f"INSERT INTO anchor.sport DEFAULT VALUES RETURNING id;")
            sport_id = gcur.fetchone()[0]

            gcur.execute(f"INSERT INTO anchor.event_sport(sport_id, event_id) VALUES ({sport_id}, {event_id});")

            gcur.execute(
                f"INSERT INTO anchor.sport_name(sport_id, value) OVERRIDING SYSTEM VALUE VALUES ({sport_id}, '{e[1]}');")
            gcur.execute(
                f"INSERT INTO anchor.event_name(event_id, value) OVERRIDING SYSTEM VALUE VALUES ({event_id}, '{e[0]}');")


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

            gcur.execute("INSERT INTO anchor.medal DEFAULT VALUES RETURNING id;")
            medal_id = gcur.fetchone()[0]

            gcur.execute(
                f"INSERT INTO anchor.medal_name(medal_id, value) OVERRIDING SYSTEM VALUE VALUES ({medal_id}, '{m}')")


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

            gcur.execute("INSERT INTO anchor.game DEFAULT VALUES RETURNING id;")
            game_id = gcur.fetchone()[0]

            gcur.execute("INSERT INTO anchor.season DEFAULT VALUES RETURNING id;")
            season_id = gcur.fetchone()[0]

            gcur.execute("INSERT INTO anchor.city DEFAULT VALUES RETURNING id;")
            city_id = gcur.fetchone()[0]

            gcur.execute(f"INSERT INTO anchor.game_season(game_id, season_id) VALUES ({game_id}, {season_id});")
            gcur.execute(f"INSERT INTO anchor.game_city(game_id, city_id) VALUES ({game_id}, {city_id});")

            gcur.execute(
                f"INSERT INTO anchor.game_name(game_id, value) OVERRIDING SYSTEM VALUE VALUES ({game_id}, '{g[0]}');")
            gcur.execute(
                f"INSERT INTO anchor.game_year(game_id, value) OVERRIDING SYSTEM VALUE VALUES ({game_id}, '{g[1]}');")
            gcur.execute(
                f"INSERT INTO anchor.season_name(season_id, value) OVERRIDING SYSTEM VALUE VALUES ({season_id}, '{g[2]}');")
            gcur.execute(
                f"INSERT INTO anchor.city_name(city_id, value) OVERRIDING SYSTEM VALUE VALUES ({city_id}, '{g[3]}');")


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
        gcur.execute("SELECT game_id, value FROM anchor.game_name")
        games = gcur.fetchall()
        gcur.execute("SELECT athlete_id, value FROM anchor.athlete_name")
        athletes = gcur.fetchall()
        gcur.execute("SELECT event_id, value FROM anchor.event_name")
        events = gcur.fetchall()
        gcur.execute("SELECT medal_id, value FROM anchor.medal_name")
        medals = gcur.fetchall()

        for p in participation:
            if p[0] is None or p[1] is None or p[2] is None:
                continue

            athlete_id = find_in_list(p[0], athletes)
            game_id = find_in_list(p[1], games)
            event_id = find_in_list(p[2], events)
            medal_id = find_in_list(p[3], medals)

            if athlete_id is None or game_id is None or event_id is None:
                continue

            # print(athlete_id, game_id, event_id, medal_id)
            if medal_id is not None:
                gcur.execute(
                    f"INSERT INTO anchor.participation(athlete_id, game_id, event_id, medal_id) VALUES ({athlete_id}, {game_id}, {event_id}, {medal_id});")
            else:
                gcur.execute(
                    f"INSERT INTO anchor.participation(athlete_id, game_id, event_id) VALUES ({athlete_id}, {game_id}, {event_id});")


def insert_result(gconn, results: list):
    """
    Inserting result table in greenplum
    :param gconn: greenplum connection
    :param results: json[{athlete_name:str, event:str, value:str, year:str}]
    :return:
    """
    with gconn.cursor() as gcur:
        gcur.execute("SELECT game_id, value FROM anchor.game_year")
        games = gcur.fetchall()
        gcur.execute("SELECT event_id, value FROM anchor.event_name")
        events = gcur.fetchall()

        for r in results:
            if r["athlete_name"] is None or r["event"] is None or "value" not in r.keys() or r["value"] is None or r["year"] is None:
                continue

            event_id = find_in_list(event_name[r["event"]], events)

            name = "%" + r['athlete_name'].replace(" ", "%").lower() + "%"
            if "'" in name:
                continue

            gcur.execute(f"SELECT athlete_id FROM anchor.athlete_name WHERE LOWER(value) LIKE '{name}'")
            athlete_id = gcur.fetchone()
            if athlete_id is None:
                continue
            else:
                athlete_id = athlete_id[0]

            year = int(r['year'])
            gcur.execute(f"SELECT game_id FROM anchor.game_name WHERE value = '{year} Summer';")
            game_id = gcur.fetchone()[0]

            gcur.execute(f"INSERT INTO anchor.result DEFAULT VALUES RETURNING id;")
            result_id = gcur.fetchone()[0]

            value = float(r['value'])
            gcur.execute(
                f"INSERT INTO anchor.result_value(result_id, value) OVERRIDING SYSTEM VALUE VALUES ({result_id}, '{value}');")

            gcur.execute(f"UPDATE anchor.participation SET result_id = {result_id} "
                         f"WHERE athlete_id = {athlete_id} AND game_id = {game_id} AND event_id = {event_id};")
