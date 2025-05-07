from connect import psql_get_connection, greenplum_get_connection
from migrate_to_datavault.greenplum import insert_event, insert_medal, insert_hub_game, insert_sat_game, \
    insert_hub_athlete, insert_sat_athlete, insert_link_participation, insert_hub_result
from migrate_to_datavault.postgresql import psql_get_athlete
from migrate_to_inmon.postgresql import psql_get_event, psql_select_dictionaries, psql_get_game, psql_get_participation
from mongo import mongo_get_collection, mongo_get_columns
from prometheus import prometheus_get_metrics_by_series


def migrate_hub_event():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download events")
    with pconn:
        events = psql_get_event(pconn)

    print("Migrate events")
    with gconn:
        insert_event(gconn, events)
        gconn.commit()


def migrate_hub_medal():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download medals")
    with pconn:
        medals = psql_select_dictionaries(pconn, "medal", "name")

    print("Migrate medals")
    with gconn:
        insert_medal(gconn, medals)
        gconn.commit()


def migrate_hub_game():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download games")
    with pconn:
        games = psql_get_game(pconn)

    print("Migrate hub_games")
    with gconn:
        insert_hub_game(gconn, games)
        gconn.commit()


def migrate_sat_game():
    mcoll = mongo_get_collection()
    gconn = greenplum_get_connection()

    print("Download games")
    games = mongo_get_columns(mcoll, "Games", ["Season", "City"])

    with gconn:
        insert_sat_game(gconn, games)


def migrate_hub_athlete():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download athlete")
    with pconn:
        athletes = psql_get_athlete(pconn)

    print("Migrate athletes")
    with gconn:
        insert_hub_athlete(gconn, athletes)


def migrate_sat_athlete():
    mcoll = mongo_get_collection()
    gconn = greenplum_get_connection()

    print("Download athlete")
    athletes = mongo_get_columns(mcoll, "Name", ["Age", "Height", "Weight", "Team", "NOC"])

    print("Migrate athletes")
    with gconn:
        insert_sat_athlete(gconn, athletes)


def migrate_link_participation():
    pconn = psql_get_connection()
    gconn = greenplum_get_connection()

    print("Download participation")
    with pconn:
        parts = psql_get_participation(pconn)

    print("Migrate participation")
    with gconn:
        insert_link_participation(gconn, parts)


def migrate_hub_result():
    gconn = greenplum_get_connection()
    print("Download results")
    results = prometheus_get_metrics_by_series("olympic_athlete_time")["data"]

    print("Migrate results")
    with gconn:
        insert_hub_result(gconn, results)


if __name__ == '__main__':
    # migrate_hub_event()
    # migrate_hub_medal()
    # migrate_hub_game()
    # migrate_sat_game()
    # migrate_hub_athlete()
    # migrate_sat_athlete()
    # migrate_link_participation()
    migrate_hub_result()
