from connect import greenplum_get_connection, psql_get_connection

from migrate_to_anchor.greenplum import insert_sex, insert_team, insert_athlete, insert_event, insert_medal, \
    insert_game, insert_participation, insert_result
from migrate_to_anchor.postgresql import psql_get_athlete, psql_get_game
from migrate_to_inmon.postgresql import psql_get_event, psql_get_participation
from mongo import mongo_get_collection, mongo_get_column, mongo_get_columns
from prometheus import prometheus_get_metrics_by_series


def migrate_sex():
    mcoll = mongo_get_collection()
    gconn = greenplum_get_connection()

    print("Download sex")
    sex = mongo_get_column(mcoll, "Sex")

    print("Migrate sex")
    with gconn:
        insert_sex(gconn, sex)
        gconn.commit()


def migrate_team():
    mcoll = mongo_get_collection()
    gconn = greenplum_get_connection()

    print("Download team")
    team = mongo_get_columns(mcoll, "Team", ["NOC"])

    print("Migrate team")
    with gconn:
        insert_team(gconn, team)
        gconn.commit()


def migrate_athlete():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download athlete")
    with pconn:
        athletes = psql_get_athlete(pconn)

    print("Migrate athlete")
    with gconn:
        insert_athlete(gconn, athletes)
        gconn.commit()


def migrate_event():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download event")
    with pconn:
        events = psql_get_event(pconn)

    print("Migrate event")
    with gconn:
        insert_event(gconn, events)
        gconn.commit()

def migrate_medal():
    mcoll = mongo_get_collection()
    gconn = greenplum_get_connection()

    print("Download medal")
    medals = mongo_get_column(mcoll, "Medal")

    print("Migrate medal")
    with gconn:
        insert_medal(gconn, medals)
        gconn.commit()

def migrate_game():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download game")
    with pconn:
        games = psql_get_game(pconn)

    print("Migrate game")
    with gconn:
        insert_game(gconn, games)
        gconn.commit()

def migrate_participation():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download participation")
    with pconn:
        parts = psql_get_participation(pconn)

    print("Migrate participation")
    with gconn:
        insert_participation(gconn, parts)
        gconn.commit()

def migrate_result():
    gconn = greenplum_get_connection()

    print("Download result")
    results = prometheus_get_metrics_by_series("olympic_athlete_time")["data"]

    print("Migrate results")
    with gconn:
        insert_result(gconn, results)
        gconn.commit()

if __name__ == '__main__':
    migrate_sex()
    migrate_team()
    migrate_medal()
    migrate_athlete()
    migrate_event()
    migrate_medal()
    migrate_game()
    migrate_participation()
    migrate_result()
