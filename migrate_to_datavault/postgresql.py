def psql_get_game(pconn) -> list:
    """
    Returns data from game table with join city and season tables
    :param pconn: postgres connection
    :return: [[game_name:str, game_year:str, season_name:str, city_name:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT g.name, g.year, s.name, c.name " +
                     "FROM kalashnikov_aa.game AS g " +
                     "JOIN city AS c ON g.city_id = c.id " +
                     "JOIN season AS s ON g.season_id = s.id")
        game = pcur.fetchall()
    return game


def psql_get_athlete(pconn) -> list:
    """
    Returns data from athlete table with join team and sex tables
    :param pconn: postgres connection
    :return: [[name:str, sex:str]]
    """
    with pconn.cursor() as pcur:
        pcur.execute("SELECT a.name, s.value "
                     "FROM kalashnikov_aa.athlete AS a "
                     "JOIN sex AS s ON a.sex_id = s.id ")
        athlete = pcur.fetchall()
    return athlete
