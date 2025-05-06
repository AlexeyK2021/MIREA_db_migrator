def insert_athlete(gconn, athlete: dict):
    """
    Inserting athlete table in greenplum
    :param gconn: greenplum connection
    :param athlete: {"Name": [name:str, sex:str, age:int, height:int, weight:int, team_name:str, noc_code:str]}
    :return:
    """
    with gconn.cursor() as gcur:
        for k, a in athlete.items():
            if k is None or a[0] is None or a[1] is None or a[2] is None or a[3] is None or a[4] is None or a[
                5] is None:
                continue
            gcur.execute(
                f"INSERT INTO kstar.athlete(name, sex, age, height, weight, team_name, noc_code) VALUES ("
                f"'{k}', '{a[0]}', {a[1]}, {a[2]}, {a[3]}, '{a[4]}', '{a[5]}');"
            )


def insert_event(gconn, event: dict):
    """
    Inserting event table in greenplum
    :param gconn: greenplum connection
    :param event: [name:str, sport_name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for e, s in event.items():
            if e is None or s[0] is None:
                continue
            gcur.execute(f"INSERT INTO kstar.event(name, sport_name) VALUES (" +
                         f"'{e}', '{s[0]}');")


def insert_game(gconn, game: list):
    """
    Inserting game table in greenplum
    :param gconn: greenplum connection
    :param game: [name:str, year:int, season:str, city:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for g in game:
            if g[0] is None or g[1] is None or g[2] is None or g[3] is None:
                continue
            gcur.execute(f"INSERT INTO kstar.game (name, year, season, city) VALUES (" +
                         f"'{g[0]}', {g[1]}, '{g[2]}', '{g[3]}');")


def insert_medal(gconn, medal: list):
    """
    Inserting medal table in greenplum
    :param gconn: greenplum connection
    :param medal: [name:str]
    :return:
    """
    with gconn.cursor() as gcur:
        for m in medal:
            if m is None:
                continue
            gcur.execute(f"INSERT INTO kstar.medal(name) VALUES ('{m}')")


def truncate_database(gconn):
    """
    Truncate database
    :param gconn: greenplum connection
    :param database_name: database name to truncate
    :return:
    """
    print(f"Truncate database Kimball Star")
    with gconn.cursor() as gcur:
        gcur.execute(f"TRUNCATE TABLE kstar.result")
        gcur.execute(f"TRUNCATE TABLE kstar.participation")
        gcur.execute(f"TRUNCATE TABLE kstar.athlete")
        gcur.execute(f"TRUNCATE TABLE kstar.game")
        gcur.execute(f"TRUNCATE TABLE kstar.event")
        gcur.execute(f"TRUNCATE TABLE kstar.medal")
