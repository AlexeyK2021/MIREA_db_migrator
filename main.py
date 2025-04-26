import struct
from msilib import schema

import psycopg2

from config import psql_db, psql_password, psql_ip, psql_user, green_db, green_user, green_password, green_ip
from prometheus import prometheus_get_metrics_by_series


def psql_get_connection():
    try:
        return psycopg2.connect(database=psql_db, user=psql_user, password=psql_password, host=psql_ip)
    except Exception as e:
        print('Can`t establish connection to database PSQL')


def greenplum_get_connection():
    try:
        return psycopg2.connect(database=green_db, user=green_user, password=green_password, host=green_ip)
    except psycopg2.Error as e:
        print('Can`t establish connection to database GREENPLUM')


def migrate_sex(pconn, gconn):
    print("Migrating sex")
    sex = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT DISTINCT value FROM kalashnikov_aa.sex")
        sex = pcur.fetchall()
    # print(sex)

    with gconn.cursor() as gcur:
        for s in sex:
            gcur.execute(f"INSERT INTO inmon.sex(value) VALUES ('{s[0]}')")


def migrate_team(pconn, gconn):
    print("Migrating team")
    team = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT team.name, noc.code " +
                     "FROM kalashnikov_aa.team " +
                     "JOIN noc ON noc.id = team.noc_id")
        team = pcur.fetchall()
    # print(team)

    with gconn.cursor() as gcur:
        for t in team:
            gcur.execute(f"INSERT INTO inmon.team(name, noc_code) VALUES ('{t[0]}', '{t[1]}')")


def migrate_athlete(pconn, gconn):
    print("Migrating athlete")
    athlete = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT a.name, s.value, a.age, a.height, a.weight, t.name " +
                     "FROM kalashnikov_aa.athlete AS a " +
                     "JOIN team AS t ON a.team_id = t.id " +
                     "JOIN sex AS s ON a.sex_id = s.id ")
        athlete = pcur.fetchall()
    print(athlete)

    with gconn.cursor() as gcur:
        for a in athlete:
            gcur.execute(
                f"INSERT INTO inmon.athlete(name, sex_id, age, height, weight, team_id) VALUES (" +
                f"'{a[0]}'," +
                f"(SELECT id FROM {schema}.sex WHERE value='{a[1]}')," +
                f"{a[2]}, {a[3]}, {a[4]}," +
                f"(SELECT id FROM {schema}.team WHERE name='{a[5]}')" +
                ")")


def migrate_city(pconn, gconn):
    print("Migrating city")
    city = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT DISTINCT name FROM kalashnikov_aa.city")
        city = pcur.fetchall()
    print(city)

    with gconn.cursor() as gcur:
        for c in city:
            gcur.execute(f"INSERT INTO inmon.city(name) VALUES ('{c[0]}')")


def migrate_season(pconn, gconn):
    print("Migrating season")
    season = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT DISTINCT name FROM kalashnikov_aa.season")
        season = pcur.fetchall()
    print(season)

    with gconn.cursor() as gcur:
        for s in season:
            gcur.execute(f"INSERT INTO inmon.season(name) VALUES ('{s[0]}')")


def migrate_game(pconn, gconn):
    print("Migrating game")
    game = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT g.name, g.year, s.name, c.name " +
                     "FROM kalashnikov_aa.game AS g " +
                     "JOIN city AS c ON g.city_id = c.id " +
                     "JOIN season AS s ON g.season_id = s.id")
        game = pcur.fetchall()
    print(game)

    with gconn.cursor() as gcur:
        for g in game:
            gcur.execute(f"INSERT INTO inmon.game (name, year, season_id, city_id) VALUES (" +
                         f"'{g[0]}', {g[1]}," +
                         f"(SELECT id FROM {schema}.season WHERE name='{g[2]}')," +
                         f"(SELECT id FROM {schema}.city WHERE name='{g[3]}')" +
                         ")")


def migrate_sport(pconn, gconn):
    print("Migrating sport")
    sport = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT DISTINCT name FROM kalashnikov_aa.sport")
        sport = pcur.fetchall()
    print(sport)

    with gconn.cursor() as gcur:
        for s in sport:
            gcur.execute(f"INSERT INTO inmon.sport(name) VALUES ('{s[0]}')")


def migrate_event(pconn, gconn):
    print("Migrating event")
    event = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT e.name, s.name FROM kalashnikov_aa.event AS e JOIN sport AS s ON e.sport_id=s.id")
        event = pcur.fetchall()
    print(event)

    with gconn.cursor() as gcur:
        for e in event:
            gcur.execute(f"INSERT INTO inmon.event(name, sport_id) VALUES (" +
                         f"'{e[0]}', " +
                         f"(SELECT id FROM inmon.sport WHERE name='{e[1]}')" +
                         ")")


def migrate_medal(pconn, gconn):
    print("Migrating medal")
    medal = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT DISTINCT name FROM kalashnikov_aa.medal")
        medal = pcur.fetchall()
    print(medal)

    with gconn.cursor() as gcur:
        for m in medal:
            gcur.execute(f"INSERT INTO inmon.medal(name) VALUES ('{m[0]}')")


def migrate_participations(pconn, gconn):
    print("Migrating participations")
    with pconn.cursor() as pcur:
        pcur.execute("SELECT * FROM kalashnikov_aa.participation_values")
        participation = pcur.fetchall()
    print(participation)

    with gconn.cursor() as gcur:
        for p in participation:
            gcur.execute(f"INSERT INTO inmon.participation(athlete_id, game_id, event_id, medal_id) VALUES ("
                         f"(SELECT id FROM inmon.athlete WHERE name='{p[0]}'), "
                         f"(SELECT id FROM inmon.game WHERE name='{p[1]}'),"
                         f"(SELECT id FROM inmon.event WHERE name='{p[2]}'), "
                         f"(SELECT id FROM inmon.medal WHERE name='{p[3]}')"
                         ")")


def migrate_result():
    pass


if __name__ == '__main__':
    psql_conn = psql_get_connection()
    green_conn = greenplum_get_connection()
    try:
        with psql_conn as pconn:
            with green_conn as gconn:
                # migrate_sex(pconn, gconn)
                # migrate_team(pconn, gconn)
                # migrate_athlete(pconn, gconn)
                # migrate_city(pconn, gconn)
                # migrate_season(pconn, gconn)
                # migrate_game(pconn, gconn)
                # migrate_sport(pconn, gconn)
                # migrate_event(pconn, gconn)
                # migrate_medal(pconn, gconn)
                migrate_participations(pconn, gconn)
                # print(prometheus_get_metrics_by_series("olympic_athlete_time"))

                gconn.commit()
                pconn.rollback()
    except Exception as e:
        print(e)
        pconn.rollback()
        psql_conn.close()
        green_conn.close()