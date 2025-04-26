import psycopg2

from config import psql_db, psql_password, psql_ip, psql_user


def psql_get_connection():
    return psycopg2.connect(database=psql_db, user=psql_user, password=psql_password, host=psql_ip)

def greenplum_get_connection():
    return

if __name__ == '__main__':
    psql_conn = psql_get_connection()

    with psql_conn.cursor() as cur:
        cur.execute('SELECT * FROM kalashnikov_aa.athlete')
        print(cur.fetchall())