from typing import List
import pycelonis.pql as pql
from pandas import DataFrame
from pycelonis.service.integration.service import ColumnTransport, ColumnType
from base.data_model import Field


def create_column_config_from_dataframe(df:DataFrame) -> List[ColumnTransport]:
    """
    creates a column config for upload to celonis from a pandas dataframe.

    params:
    ------
    df: the dataframe to be uploaded
    """
    pass

def transform_columns_to_pql_query(columns:List[Field]) -> pql.PQL:
    """
    transforms a list of string columns in pql format into a pql query
    ready for extraction.

    params:
    -------
    columns: list of column names in pql format
    """
    query = pql.PQL()
    for col in columns:
        query += pql.PQLColumn(name=col.column, query=col.pql)
    
    return query