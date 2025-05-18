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