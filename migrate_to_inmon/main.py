from connect import psql_get_connection, greenplum_get_connection
from greenplum import insert_sex, insert_team, insert_athlete, insert_city, insert_season, \
    insert_game, insert_sport, \
    insert_event, insert_medal, insert_participation, insert_result, insert_noc
from mongo import mongo_get_collection, mongo_get_column, mongo_get_columns
from postgresql import psql_get_team, psql_get_athlete, psql_get_game, psql_get_event, psql_get_participation
from prometheus import prometheus_get_metrics_by_series


def migrate_sex(pconn, gconn, mcoll):
    print("Migrating sex")
    # sex = psql_select_dictionaries(pconn, "sex", "value")
    sex = mongo_get_column(mcoll, "Sex")
    insert_sex(gconn, sex)


def migrate_noc(pconn, gconn, mcoll):
    print("Migrating NOC")
    noc = mongo_get_column(mcoll, "NOC")
    insert_noc(gconn, noc)


def migrate_team(pconn, gconn, mcoll):
    print("Migrating team")
    teams = psql_get_team(pconn)
    insert_team(gconn, teams)


def migrate_athlete(pconn, gconn, mcoll):
    print("Migrating athlete")
    athlete = psql_get_athlete(pconn)
    insert_athlete(gconn, athlete)


def migrate_city(pconn, gconn, mcoll):
    print("Migrating city")
    # city = psql_select_dictionaries(pconn, "city", "name")
    city = mongo_get_column(mcoll, "City")
    insert_city(gconn, city)


def migrate_season(pconn, gconn, mcoll):
    print("Migrating season")
    # season = psql_select_dictionaries(pconn, "season", "name")
    season = mongo_get_column(mcoll, "Season")
    insert_season(gconn, season)


def migrate_game(pconn, gconn, mcoll):
    print("Migrating game")
    game = psql_get_game(pconn)
    insert_game(gconn, game)


def migrate_sport(pconn, gconn, mcoll):
    print("Migrating sport")
    # sport = psql_select_dictionaries(pconn, "sport", "name")
    sport = mongo_get_column(mcoll, "Sport")
    insert_sport(gconn, sport)


def migrate_event(pconn, gconn, mcoll):
    print("Migrating event")
    event = psql_get_event(pconn)
    insert_event(gconn, event)


def migrate_medal(pconn, gconn, mcoll):
    print("Migrating medal")
    # medal = psql_select_dictionaries(pconn, "medal", "name")
    medal = mongo_get_column(mcoll, "Medal")
    insert_medal(gconn, medal)


def migrate_participation(pconn, gconn, mcoll):
    print("Migrating participation")
    participation = psql_get_participation(pconn)
    insert_participation(gconn, participation)


def migrate_result(gconn):
    print("Migrate results")
    results = prometheus_get_metrics_by_series("olympic_athlete_time")["data"]
    insert_result(gconn, results)


def sandbox(mcol):
    print(mongo_get_columns(mcol, ["Team", "NOC"]))


if __name__ == '__main__':
    psql_conn = psql_get_connection()
    green_conn = greenplum_get_connection()
    mongo_coll = mongo_get_collection()

    with green_conn as gconn:  # добавить получение conn и commit в каждую функцию миграции
        with psql_conn as pconn:
            # truncate_database(gconn, "inmon")
            # gconn.commit()
            # migrate_sex(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_noc(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_team(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_athlete(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_city(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_season(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_game(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_sport(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_event(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_medal(pconn, gconn, mongo_coll)
            # gconn.commit()
            # migrate_participation(pconn, gconn, mongo_coll)
            # gconn.commit()
            migrate_result(gconn)
            gconn.commit()
