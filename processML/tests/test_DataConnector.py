import unittest
from utils.data_extraction import transform_columns_to_pql_query
import pycelonis.pql as pql
from base.data_model import Field
from base.data_connector import CeloConnector

class TestDataConnector(unittest.TestCase):

    def setUp(self):
        return super().setUp()
    
    def tearDown(self):
        return super().tearDown()
    
    def test_transform_columns_to_pql_query(self):
        """
        tests the helper function that transforms field objects 
        into pql columns
        """
        columns = [
            Field(table='o_celonis_SalesOrder', column='NetAmount'),
            Field(table='o_celonis_SalesOrder', column='DocumentType'),
            Field(table='o_celonis_SalesOrder', column='SalesOrg'),
        ]
        query = transform_columns_to_pql_query(columns)
 
        self.assertIsInstance(query, pql.PQL)
        self.assertEqual(len(query.columns), 3)
        

    def test_data_connector_init_connection(self):
        """
        ensures correct error handling during intialisation of 
        CeloConnector object
        """
