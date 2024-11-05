from typing import List, Optional
import pycelonis.pql as pql
from base.data_model import Field, Filter

def create_pql_query(fields:List[Field], filter:Optional[Filter]=None) -> pql.PQL:
    """
    transforms a list of fields and filters into a pql query
    ready for extraction.

    params:
    -------
    fields: list of field objects
    filter: an optional filter
    """
    query = pql.PQL()
    for field in fields:
        query += pql.PQLColumn(name=field.column, query=field.pql)
    
    if filter:
        query += pql.PQLFilter(query=filter.pql)

    return query