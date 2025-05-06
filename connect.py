import json

import psycopg2
import requests

VbIp = "192.168.1.120"

# psqlUrl = "postgres://kalashnikov_aa:nTCv&%25eX&q@7bd8a-rw.db.pub.dbaas.postgrespro.ru/dbstud"
psql_ip = "7bd8a-rw.db.pub.dbaas.postgrespro.ru"
psql_user = "kalashnikov_aa"
psql_password = "nTCv&%eX&q"
psql_db = "dbstud"

# mongoUrl = f"mongodb://{VbIp}:27017/"
prometheusUrl = f"http://{VbIp}:9090/api/v1/"

# greenplumUrl = f"postgres://gpadmin:dataroad@{VbIp}:5432/mireadb"
green_ip = VbIp
green_user = "gpadmin"
green_password = "dataroad"
green_db = "mireadb"


def psql_get_connection():
    try:
        return psycopg2.connect(database=psql_db, user=psql_user, password=psql_password, host=psql_ip)
    except Exception as e:
        print('Can`t establish connection to database PSQL')


def greenplum_get_connection():
    try:
        return psycopg2.connect(database=green_db, user=green_user, password=green_password, host=green_ip)
    except psycopg2.Error as e:
        print('Can`t establish connection to database GREENPLUM')


def prometheus_get_metrics_by_series(serie):
    url = prometheusUrl + f"series?match[]={serie}"
    response = requests.get(url)
    body = response.text
    json_data = json.loads(body)
    return json_data
