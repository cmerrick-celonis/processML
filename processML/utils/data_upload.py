from pandas import DataFrame
from pycelonis.service.integration.service import ColumnTransport, ColumnType
from typing import List

def create_column_config_from_dataframe(df:DataFrame) -> List[ColumnTransport]:
    """
    creates a column config for upload to celonis from a pandas dataframe.

    params:
    ------
    df: the dataframe to be uploaded
    """
    pass