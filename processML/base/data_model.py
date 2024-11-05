from pydantic import BaseModel, field_validator
import regex as re

class Field(BaseModel):
    """
    Represents a field in the celonis data model
    """
    table: str
    column: str

    @property
    def pql(self):
        """
        the pql query for this field
        """
        return f'"{self.table}"."{self.column}"'

class Filter(BaseModel):
    """
    filter to apply to a query.
    must be in the format 'Filter Condition;'
    """
    pql: str

    @field_validator('pql')
    @classmethod
    def name_is_correct_pql(cls, pql:str):
        if re.match(pattern=r'Filter .*;', string=pql):
            return pql
        else:
            raise ValueError('Filter is not correct PQL')



    def to_not(self):
        not_pql = self.pql[:6] + ' ' + 'NOT' + ' ' + self.pql[7:]
        return Filter(pql=not_pql)

