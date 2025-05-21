import psycopg2

from config import green_db, green_user, green_password, green_ip


def greenplum_get_connection():
    try:
        return psycopg2.connect(database=green_db, user=green_user, password=green_password, host=green_ip)
    except psycopg2.Error as e:
        print('Can`t establish connection to database GREENPLUM')


def greenplum_get_all(gconn) -> list:
    with gconn.cursor() as pcur:
        pcur.execute("SELECT * FROM inmon.athletes_info")
        athlete = pcur.fetchall()
    return athlete


def greenplum_get_athlete(gconn):
    with gconn.cursor() as gcur:
        gcur.execute("SELECT name, age, sex, height, weight, team_name, noc_code FROM kstar.athlete")
        athlete = gcur.fetchall()
    return athlete


def greenplum_get_event(gconn):
    with gconn.cursor() as gcur:
        gcur.execute("SELECT name, sport_name FROM kstar.event")
        event = gcur.fetchall()
    return event


def greenplum_get_game(gconn):
    with gconn.cursor() as gcur:
        gcur.execute("SELECT year, season, city, name FROM kstar.game")
        game = gcur.fetchall()
    return game


def greenplum_get_medal(gconn):
    with gconn.cursor() as gcur:
        gcur.execute("SELECT name FROM kstar.medal")
        medal = gcur.fetchall()
    return medal


def greenplum_get_result(gconn):
    with gconn.cursor() as gcur:
        gcur.execute("SELECT value FROM kstar.result")
        result = gcur.fetchall()
    return result


def greenplum_get_participation(gconn):
    with gconn.cursor() as gcur:
        gcur.execute("SELECT a.name, g.name, e.name, m.name, r.value FROM kstar.participation AS p "
                     "JOIN kstar.athlete as a ON p.athlete_id = a.id "
                     "JOIN kstar.event e on p.event_id = e.id "
                     "JOIN kstar.game g on g.id = p.game_id "
                     "LEFT JOIN kstar.medal m on p.medal_id = m.id "
                     "LEFT JOIN kstar.result r on p.result_id = r.id")
        data = gcur.fetchall()
    return data
