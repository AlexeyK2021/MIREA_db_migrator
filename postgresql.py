import psycopg2

from config import psql_db, psql_user, psql_password, psql_ip


def psql_get_connection():
    try:
        return psycopg2.connect(database=psql_db, user=psql_user, password=psql_password, host=psql_ip)
    except Exception as e:
        print('Can`t establish connection to database PSQL')


def psql_select_dictionaries(pconn, table_name):
    print(f"Scanning {table_name}")
    with pconn.cursor() as pcur:
        pcur.execute(f"SELECT DISTINCT * FROM kalashnikov_aa.{table_name}")
    return pcur.fetchall()
