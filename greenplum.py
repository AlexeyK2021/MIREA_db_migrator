import psycopg2

from config import green_db, green_user, green_password, green_ip
from prom_green_value_connector import event_name


def greenplum_get_connection():
    try:
        return psycopg2.connect(database=green_db, user=green_user, password=green_password, host=green_ip)
    except psycopg2.Error as e:
        print('Can`t establish connection to database GREENPLUM')


def insert_sex(gconn, sex: set):
    """
    Inserting sex table in greenplum
    :param gconn: greenplum connection
    :param sex: [sex:str]
    :return: nothing
    """
    with gconn.cursor() as gcur:
        for s in sex:
            gcur.execute(f"INSERT INTO inmon.sex(value) VALUES ('{s}')")


def insert_team(gconn, team: set):
    """
    Inserting team table in greenplum
    :param gconn: greenplum connection
    :param team: [(name:str, noc_code:str)]
    :return:
    """
    with gconn.cursor() as gcur:
        for t in team:
            gcur.execute(f"INSERT INTO inmon.team(name, noc_code) VALUES ('{t[0]}', '{t[1]}')")


def insert_athlete(gconn, athlete: set):
    """
    Inserting athlete table in greenplum
    :param gconn: greenplum connection
    :param athlete: [(name:str, sex:str, age:int, height:int, weight:int, team:str)]
    :return:
    """
    with gconn.cursor() as gcur:
        for a in athlete:
            gcur.execute(
                f"INSERT INTO inmon.athlete(name, sex_id, age, height, weight, team_id) VALUES (" +
                f"'{a[0]}'," +
                f"(SELECT id FROM inmon.sex WHERE value='{a[1]}')," +
                f"{a[2]}, {a[3]}, {a[4]}," +
                f"(SELECT id FROM inmon.team WHERE name='{a[5]}')" +
                ")")


def insert_city(gconn, city: set):
    """
    Inserting city table in greenplum
    :param gconn: greenplum connection
    :param city: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for c in city:
            gcur.execute(f"INSERT INTO inmon.city(name) VALUES ('{c}')")


def insert_season(gconn, season: set):
    """
    Inserting season table in greenplum
    :param gconn: greenplum connection
    :param season: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for s in season:
            gcur.execute(f"INSERT INTO inmon.season(name) VALUES ('{s}')")


def insert_game(gconn, game: list):
    """
    Inserting game table in greenplum
    :param gconn: greenplum connection
    :param game: [name:str, year:int, season:str, city:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for g in game:
            gcur.execute(f"INSERT INTO inmon.game (name, year, season_id, city_id) VALUES (" +
                         f"'{g[0]}', {g[1]}," +
                         f"(SELECT id FROM inmon.season WHERE name='{g[2]}')," +
                         f"(SELECT id FROM inmon.city WHERE name='{g[3]}')" +
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
            gcur.execute(f"INSERT INTO inmon.sport(name) VALUES ('{s}')")


def insert_event(gconn, event: list):
    """
    Inserting event table in greenplum
    :param gconn: greenplum connection
    :param event: [name:str, sport:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for e in event:
            gcur.execute(f"INSERT INTO inmon.event(name, sport_id) VALUES (" +
                         f"'{e[0]}', " +
                         f"(SELECT id FROM inmon.sport WHERE name='{e[1]}')" +
                         ")")


def insert_medal(gconn, medal: list):
    """
    Inserting medal table in greenplum
    :param gconn: greenplum connection
    :param medal: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for m in medal:
            gcur.execute(f"INSERT INTO inmon.medal(name) VALUES ('{m}')")


def insert_participation(gconn, participation: list):
    """
    Inserting participation table in greenplum
    :param gconn: greenplum connection
    :param participation: [athlete_name:str, game_name:str, event_name:str, medal_name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for p in participation:
            gcur.execute(f"INSERT INTO inmon.participation(athlete_id, game_id, event_id, medal_id) VALUES ("
                         f"(SELECT id FROM inmon.athlete WHERE name='{p[0]}'), "
                         f"(SELECT id FROM inmon.game WHERE name='{p[1]}'),"
                         f"(SELECT id FROM inmon.event WHERE name='{p[2]}'), "
                         f"(SELECT id FROM inmon.medal WHERE name='{p[3]}')"
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

            gcur.execute(f"SELECT id FROM inmon.athlete WHERE LOWER(name) LIKE '{name}'")
            athlete_id = gcur.fetchone()
            if athlete_id is None:
                continue
            else:
                athlete_id = athlete_id[0]

            event_ = event_name[result['event']]
            gcur.execute(f"SELECT id FROM inmon.event WHERE name LIKE '{event_}'")
            event_id = gcur.fetchone()[0]

            year = int(result['year'])
            gcur.execute(
                f"SELECT id FROM inmon.game WHERE season_id = (SELECT id FROM inmon.season WHERE name = 'Summer') AND year = {year}")
            game_id = gcur.fetchone()[0]

            gcur.execute(
                f"SELECT id FROM inmon.participation WHERE athlete_id = {athlete_id} AND game_id = {game_id} AND event_id = {event_id}")
            part_id = gcur.fetchone()
            if part_id is None:
                continue
            else:
                part_id = part_id[0]

            if "value" not in result:
                continue
            value = float(result['value'])
            gcur.execute(f"INSERT INTO inmon.result(participation_id, value) VALUES ({part_id}, {value})")


def truncate_database(gconn, database_name):
    """
    Truncate database
    :param gconn: greenplum connection
    :param database_name: database name to truncate
    :return:
    """
    print(f"Truncate database {database_name}")
    with gconn.cursor() as gcur:
        if database_name == "inmon":
            gcur.execute(f"TRUNCATE TABLE {database_name}.result")
            gcur.execute(f"TRUNCATE TABLE {database_name}.participation")
            gcur.execute(f"TRUNCATE TABLE {database_name}.athlete")
            gcur.execute(f"TRUNCATE TABLE {database_name}.game")
            gcur.execute(f"TRUNCATE TABLE {database_name}.event")
            gcur.execute(f"TRUNCATE TABLE {database_name}.medal")
            gcur.execute(f"TRUNCATE TABLE {database_name}.sport")
            gcur.execute(f"TRUNCATE TABLE {database_name}.season")
            gcur.execute(f"TRUNCATE TABLE {database_name}.sport")
            gcur.execute(f"TRUNCATE TABLE {database_name}.city")
            gcur.execute(f"TRUNCATE TABLE {database_name}.sex")
            gcur.execute(f"TRUNCATE TABLE {database_name}.team")
