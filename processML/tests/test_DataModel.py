import unittest
from base.data_model import Field, Filter
from pydantic_core import ValidationError

class TestFieldClass(unittest.TestCase):

    def test_string_method(self):
        field = Field(table='o_celonis_SalesOrder', column='NetAmount')
        expected = '"o_celonis_SalesOrder"."NetAmount"'
        self.assertEqual(field.pql, expected)


class TestFilterClass(unittest.TestCase):

    def test_validation_error(self):
        """
        A valid filter object should only accept properly formatted pql.
        """
        with self.assertRaises(ValidationError):
            Filter(pql='"o_celonis_SalesOrder"."NetAmount" > 0')

    def test_correct_pql(self):
        """
        A valid filter object should only accept properly formatted pql.
        """
        Filter(pql='Filter "o_celonis_SalesOrder"."NetAmount" > 0;')
    
    def test_to_not_method(self):
        """
        To not should return the negated filter
        """
        pql_filter = Filter(pql='Filter "o_celonis_SalesOrder"."NetAmount" > 0;')
        not_filter = pql_filter.to_not()
        self.assertEqual(not_filter.pql, 'Filter NOT "o_celonis_SalesOrder"."NetAmount" > 0;')



                        