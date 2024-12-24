from returns.result import Success
from returns.result import Result, Success, Failure
from app.db.neo4j_database import driver


def get_target_type_with_group_attack() -> Result:
    with driver.session() as session:
        query = """
            MATCH (t:Target)<-[:EVENT]-(a:Attack_grope)
            RETURN t.target_type AS Target, collect(a.name) AS AttackGroups
        """
        try:
            res = session.run(query).data()
            data = [{"Target": record["Target"], "AttackGroups": record["AttackGroups"]} for record in res]
            return  Success(data)
        except Exception as e:
            return Failure(f"An error occurred: {str(e)}")


def get_groups_by_country() -> Result:
    query = """
        MATCH (c:Country)<-[:EVENT]-(a:Attack_grope)
        RETURN c.name AS Country, collect(a.name) AS AttackGroups
    """
    try:
        with driver.session() as session:
            result = session.run(query)
            data = [{"Country": record["Country"], "AttackGroups": record["AttackGroups"]} for record in result]
            return Success(data)
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")




def get_targets_by_group_and_year(year: int) -> Result:
    query = """
        MATCH (t:Target)<-[:EVENT {year: $year}]-(a:Attack_grope)
        RETURN t.target_type AS Target, collect(a.name) AS AttackGroups
    """
    try:
        with driver.session() as session:
            result = session.run(query, year=year)
            data = [{"Target": record["Target"], "AttackGroups": record["AttackGroups"]} for record in result]
            return Success(data)
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")



