from msilib import schema

from config import\
    mongo_get_collection
from greenplum import insert_sex, insert_team, insert_athlete, insert_city, insert_season, insert_game, insert_sport, \
    insert_event, insert_medal, insert_participation, insert_result, greenplum_get_connection
from postgresql import psql_select_dictionaries


def migrate_sex(pconn, gconn):
    print("Migrating sex")
    sex = psql_select_dictionaries(pconn, "sex")
    insert_sex(gconn, sex)


def migrate_team(pconn, gconn):
    print("Migrating team")
    with pconn.cursor() as pcur:
        pcur.execute("SELECT team.name, noc.code " +
                     "FROM kalashnikov_aa.team " +
                     "JOIN noc ON noc.id = team.noc_id")
        team = pcur.fetchall()
    # print(team)

    insert_team(gconn, team)


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

    insert_athlete(gconn, athlete)


def migrate_city(pconn, gconn):
    print("Migrating city")
    city = psql_select_dictionaries(pconn, "city")
    insert_city(gconn, city)


def migrate_season(pconn, gconn):
    print("Migrating season")
    season = psql_select_dictionaries(pconn, "season")
    insert_season(gconn, season)


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

    insert_game(gconn, game)


def migrate_sport(pconn, gconn):
    print("Migrating sport")
    sport = psql_select_dictionaries(pconn, "sport")
    insert_sport(gconn, sport)


def migrate_event(pconn, gconn):
    print("Migrating event")
    event = list()
    with pconn.cursor() as pcur:
        pcur.execute("SELECT e.name, s.name FROM kalashnikov_aa.event AS e JOIN sport AS s ON e.sport_id=s.id")
        event = pcur.fetchall()
    print(event)

    insert_event(gconn, event)


def migrate_medal(pconn, gconn):
    print("Migrating medal")
    medal = psql_select_dictionaries(pconn, "medal")

    insert_medal(gconn, medal)


def migrate_participation(pconn, gconn):
    print("Migrating participation")
    participation = psql_select_dictionaries(pconn, "participation")
    insert_participation(gconn, participation)


def migrate_result(gconn):
    print("Migrate results")
    results = prometheus_get_metrics_by_series("olympic_athlete_time")["data"]
    insert_result(gconn, results)


def sandbox(mcol):
    for a in mcol.find({"Name": "Viktor Andreyevich Aboimov"}):
        print(a)


if __name__ == '__main__':
    psql_conn = psql_get_connection()
    green_conn = greenplum_get_connection()
    mongo_coll = mongo_get_collection()

    with green_conn as gconn:
        with psql_conn as pconn:
            # migrate_sex(pconn, gconn)
            # migrate_team(pconn, gconn)
            # migrate_athlete(pconn, gconn)
            # migrate_city(pconn, gconn)
            # migrate_season(pconn, gconn)
            # migrate_game(pconn, gconn)
            # migrate_sport(pconn, gconn)
            # migrate_event(pconn, gconn)
            # migrate_medal(pconn, gconn)
            # migrate_participations(pconn, gconn)
            # print(prometheus_get_metrics_by_series("olympic_athlete_time"))
            migrate_result(gconn)
            # sandbox()
            # pconn.rollback()

        sandbox(mongo_coll)
        gconn.commit()
