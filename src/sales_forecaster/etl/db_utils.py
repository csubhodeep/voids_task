import sqlalchemy.engine
from sqlalchemy.engine import create_engine

from sales_forecaster.utils.params import connection_string


def get_db_engine(url: str = connection_string) -> sqlalchemy.engine.Engine:
    return create_engine(connection_string)