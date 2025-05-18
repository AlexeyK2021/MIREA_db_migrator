from greenplum_kstar_to_clickhouse.clickhouse import get_client, insert_all
from greenplum_kstar_to_clickhouse.greenplum import greenplum_get_connection, greenplum_get_all


def migrate_athlete():
    cl = get_client()
    gconn = greenplum_get_connection()

    print("Download athlete")
    with gconn:
        data = greenplum_get_all(gconn)

    print(len(data))
    print("Upload athlete")
    insert_all(cl, data)


if __name__ == '__main__':
    migrate_athlete()
