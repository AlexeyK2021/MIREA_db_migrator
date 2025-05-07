from connect import greenplum_get_connection, psql_get_connection
from migrate_to_inmon.postgresql import psql_get_game, psql_select_dictionaries, psql_get_participation
from migrate_to_kstar.greenplum import insert_athlete, insert_event, insert_game, insert_medal, insert_result, \
    insert_participation
from mongo import mongo_get_collection, mongo_get_columns
from prometheus import prometheus_get_metrics_by_series


def migrate_athlete():
    gconn = greenplum_get_connection()
    mcoll = mongo_get_collection()

    print("Download Athletes from MongoDB")
    athletes = mongo_get_columns(mcoll, "Name", ["Age", "Sex", "Height", "Weight", "Team", "NOC"])

    print("Writing Athletes to Greenplum")
    with gconn:
        insert_athlete(gconn, athletes)
        gconn.commit()


def migrate_event():
    gconn = greenplum_get_connection()
    mcoll = mongo_get_collection()

    print("Download Events from MongoDB")
    events = mongo_get_columns(mcoll, "Event", ["Sport"])

    print("Writing Events to Greenplum")
    with gconn:
        insert_event(gconn, events)
        gconn.commit()


def migrate_game():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download Games from PostgreSQL")
    with pconn:
        games = psql_get_game(pconn)

    print("Writing Games to Greenplum")
    with gconn:
        insert_game(gconn, games)
        gconn.commit()


def migrate_medal():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download Medals from PostgreSQL")
    with pconn:
        medals = psql_select_dictionaries(pconn, "medal", "name")

    print("Writing Games to Greenplum")
    with gconn:
        insert_medal(gconn, medals)
        gconn.commit()


def migrate_participation():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download Participation from PostgreSQL")
    with pconn:
        parts = psql_get_participation(pconn)

    print("Writing Participation to Greenplum")
    with gconn:
        insert_participation(gconn, parts)
        gconn.commit()


def migrate_result():
    print("Download Results from Prometheus")
    gconn = greenplum_get_connection()
    results = prometheus_get_metrics_by_series("olympic_athlete_time")["data"]

    print("Writing Results to Greenplum")
    with gconn:
        insert_result(gconn, results)
        gconn.commit()


if __name__ == '__main__':
    # pconn = psql_get_connection()
    # gconn = greenplum_get_connection()
    # migrate_athlete()
    # migrate_event()
    # migrate_game()
    # migrate_medal()
    # migrate_participation()
    # migrate_result()
    # mcoll = mongo_get_collection()
    # c = mongo_get_columns(mcoll, "Name", ["Age", "Height", "Weight", "Team", "NOC"])
    # for k, v in c.items():
    #     print(f"{k} -> {v}")
    # print(prometheus_get_metrics_by_series("olympic_athlete_time")["data"])
