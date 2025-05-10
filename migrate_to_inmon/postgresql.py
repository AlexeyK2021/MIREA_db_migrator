def psql_select_dictionaries(pconn, table_name: str, column_name: str) -> set:
    """
    Selects all tables without FK
    :param pconn: postgres connection
    :param table_name: name of table to select data from
    :return: list of lists of data [[arg1:any, arg2:any, ...]]
    """
    print(f"Scanning {table_name}")
    with pconn.cursor() as pcur:
        pcur.execute(f"SELECT DISTINCT {column_name} FROM kalashnikov_aa.{table_name}")
        data = set(pcur.fetchall())
        data = {item[0] for item in data}
    return data


def psql_get_team(pconn) -> list:
    """
    Returns data from team table with join noc table
    :param pconn: postgres connection
    :return: [[team_name:str, noc_code:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT team.name, noc.code " +
                     "FROM kalashnikov_aa.team " +
                     "JOIN noc ON noc.id = team.noc_id")
        team = pcur.fetchall()
    return team


def psql_get_athlete(pconn) -> list:
    """
    Returns data from athlete table with join team and sex tables
    :param pconn: postgres connection
    :return: [[name:str, sex:str, age:int, height:int, weight:int, team_name:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT a.name, s.value, a.age, a.height, a.weight, t.name " +
                     "FROM kalashnikov_aa.athlete AS a " +
                     "JOIN team AS t ON a.team_id = t.id " +
                     "JOIN sex AS s ON a.sex_id = s.id ")
        athlete = pcur.fetchall()
    return athlete


def psql_get_game(pconn) -> list:
    """
    Returns data from game table
    :param pconn: postgres connection
    :return: [[game_name:str, game_year:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT g.name, g.year " +
                     "FROM kalashnikov_aa.game AS g ")
        game = pcur.fetchall()
    return game


def psql_get_event(pconn) -> list:
    """
    Returns data from event table with join sport table
    :param pconn: postgres connection
    :return: [[event_name:str, sport_name:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT e.name, s.name FROM kalashnikov_aa.event AS e JOIN sport AS s ON e.sport_id=s.id")
        event = pcur.fetchall()
    return event


def psql_get_participation(pconn) -> list:
    """
    Returns data from participation table
    :param pconn: postgres connection
    :return: list[athlete_name:str, game_name:str, event_name:str, medal_name:str]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT * FROM kalashnikov_aa.participation_values_na")
        participation = pcur.fetchall()
    return participation
