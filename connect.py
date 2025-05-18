import json

import psycopg2
import requests

from config import psql_db, psql_user, psql_password, psql_ip, green_db, green_user, green_password, green_ip


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

