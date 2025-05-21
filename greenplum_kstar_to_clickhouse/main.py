from greenplum_kstar_to_clickhouse.clickhouse import get_client, insert_all, insert_athlete, insert_game, insert_event, \
    insert_medal, insert_result, insert_participation
from greenplum_kstar_to_clickhouse.greenplum import greenplum_get_connection, greenplum_get_all, greenplum_get_athlete, \
    greenplum_get_medal, greenplum_get_game, greenplum_get_result, greenplum_get_participation


def migrate_all():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download ALL")
    with gconn:
        data = greenplum_get_all(gconn)

    print(len(data))
    print("Upload ALL")
    insert_all(cl, data)


def migrate_athlete():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download ATHLETE")
    with gconn:
        data = greenplum_get_athlete(gconn)

    print("Upload ATHLETE")
    insert_athlete(cl, data)


def migrate_event():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download EVENT")
    with gconn:
        data = greenplum_get_medal(gconn)

    print("Upload EVENT")
    insert_event(cl, data)


def migrate_game():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download GAME")
    with gconn:
        data = greenplum_get_game(gconn)

    print("Upload GAME")
    insert_game(cl, data)


def migrate_medal():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download MEDAL")
    with gconn:
        data = greenplum_get_medal(gconn)

    print("Upload MEDAL")
    insert_medal(cl, data)


def migrate_result():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download RESULT")
    with gconn:
        data = greenplum_get_result(gconn)

    print("Upload RESULT")
    insert_result(cl, data)

def migrate_participation():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download RESULT")
    with gconn:
        data = greenplum_get_participation(gconn)

    print("Upload RESULT")
    insert_participation(cl, data)

if __name__ == '__main__':
    # migrate_all()
    # migrate_athlete()
    # migrate_event()
    # migrate_game()
    # migrate_medal()
    # migrate_result()
    migrate_participation()
