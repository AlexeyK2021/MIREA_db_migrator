# def psql_get_athlete(pconn) -> list:
#     """
#     Returns data from athlete table with join team and sex tables
#     :param pconn: postgres connection
#     :return: [[name:str, sex:str, age:int, height:int, weight:int, team_name:str, noc_code:str]]
#     """
#     with pconn.cursor() as pcur:
#         pcur.execute("SELECT a.name, s.value, a.age, a.height, a.weight, t.name, n.code " +
#                      "FROM kalashnikov_aa.athlete AS a " +
#                      "JOIN team AS t ON a.team_id = t.id " +
#                      "JOIN sex AS s ON a.sex_id = s.id " +
#                      "JOIN noc AS n ON t.noc_id = n.id ")
#         athlete = pcur.fetchall()
#     return athlete
