from pydantic import BaseModel

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
