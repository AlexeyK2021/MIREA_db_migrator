def psql_get_athlete(pconn) -> list:
    """
    Returns data from athlete table with join team and sex tables
    :param pconn: postgres connection
    :return: [[name:str, sex:str, age:int, height:int, weight:int, team:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT a.name, s.value, a.age, a.height, a.weight, t.name "
                     "FROM kalashnikov_aa.athlete AS a "
                     "JOIN sex AS s ON a.sex_id = s.id "
                     "JOIN team AS t ON a.team_id = t.id")
        athlete = pcur.fetchall()
    return athlete


def psql_get_game(pconn) -> list:
    """
    Returns data from game table with join city and season tables
    :param pconn: postgres connection
    :return: [[game_name:str, game_year:str, season_name:str, city_name:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT g.name, g.year, c.name, s.name " +
                     "FROM kalashnikov_aa.game AS g "
                     "JOIN city AS c ON g.city_id = c.id "
                     "JOIN season AS s ON g.season_id = s.id;")
        game = pcur.fetchall()
    return game
