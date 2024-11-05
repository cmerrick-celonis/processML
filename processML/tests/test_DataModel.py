import unittest
from base.data_model import Field

class TestFieldClass(unittest.TestCase):

    def test_string_method(self):
        field = Field(table='o_celonis_SalesOrder', column='NetAmount')
        expected = '"o_celonis_SalesOrder"."NetAmount"'
        self.assertEqual(field.pql, expected)