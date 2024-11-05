"""
Implements the base class for building ML jobs in Celonis
"""

import unittest
from unittest.mock import patch, MagicMock
from utils.data_extraction import create_pql_query
import pycelonis.pql as pql
from base.data_model import Field, Filter
from base.data_connector import CeloConnector
from pycelonis_core.utils.errors import PyCelonisNotFoundError, PyCelonisPermissionError

class TestDataConnector(unittest.TestCase):

    def setUp(self):
        self.base_url = "https://example.celonis.com"
        self.api_token = "dummy_token"
        self.key_type = "USER_KEY"
        self.data_pool_id = "mock_data_pool_id"
        self.data_model_id = "mock_data_model_id"
        self.space_name = "mock_space"
        self.package_name = "mock_package"
        self.knowledge_model_name = "mock_knowledge_model"
        
        
        return super().setUp()
    
    def tearDown(self):
        return super().tearDown()
        
    @patch('base.data_connector.get_celonis')
    @patch('base.data_connector.KnowledgeModelSaolaConnector')
    def test_init_success(self, MockKnowledgeModelSaolaConnector, mock_get_celonis):
        """
        ensures the init function works as expected upon a successful connection
        """
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance

        mock_data_pool = MagicMock()
        mock_data_model =  MagicMock()
        mock_data_pool.get_data_model.return_value = mock_data_model
        mock_celonis_instance.data_integration.get_data_pool.return_value = mock_data_pool

        mock_space = MagicMock()
        mock_package = MagicMock()
        mock_knowledge_model = MagicMock()
        mock_celonis_instance.studio.get_spaces.return_value.find.return_value = mock_space
        mock_space.get_packages.return_value.find.return_value = mock_package
        mock_package.get_knowledge_models.return_value.find.return_value = mock_knowledge_model

        connector = CeloConnector(
            base_url=self.base_url,
            api_token=self.api_token,
            key_type=self.key_type,
            data_pool_id=self.data_pool_id,
            data_model_id=self.data_model_id,
            space_name=self.space_name,
            package_name=self.package_name,
            knowledge_model_name=self.knowledge_model_name
        )

        #Assertions
        mock_get_celonis.assert_called_once_with(self.base_url, self.api_token, self.key_type)
        mock_celonis_instance.data_integration_get_data_pool_assert_called_once_with(self.data_pool_id)
        mock_data_pool.get_data_model.assert_called_once_with(self.data_model_id)

        mock_celonis_instance.studio.get_spaces.assert_called_once()
        mock_space.get_packages.return_value.find.assert_called_once_with(self.package_name)
        mock_package.get_knowledge_models.return_value.find.assert_called_once_with(self.knowledge_model_name)
        MockKnowledgeModelSaolaConnector.assert_called_once_with(mock_data_model, mock_knowledge_model)

    @patch('base.data_connector.get_celonis')
    def test_data_pool_permission_error(self, mock_get_celonis):
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance

        mock_celonis_instance.data_integration.get_data_pool.side_effect = PyCelonisPermissionError

        with self.assertRaises(PyCelonisPermissionError):
            CeloConnector(
                base_url=self.base_url,
                api_token=self.api_token,
                key_type=self.key_type,
                data_pool_id=self.data_pool_id,
                data_model_id=self.data_model_id,
                space_name=self.space_name,
                package_name=self.package_name,
                knowledge_model_name=self.knowledge_model_name
            )
    
    @patch('base.data_connector.get_celonis')
    def test_data_pool_not_found_error(self, mock_get_celonis):
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance

        mock_celonis_instance.data_integration.get_data_pool.side_effect = PyCelonisNotFoundError

        with self.assertRaises(PyCelonisNotFoundError):
            CeloConnector(
                base_url=self.base_url,
                api_token=self.api_token,
                key_type=self.key_type,
                data_pool_id=self.data_pool_id,
                data_model_id=self.data_model_id,
                space_name=self.space_name,
                package_name=self.package_name,
                knowledge_model_name=self.knowledge_model_name
            )
    
    @patch('base.data_connector.get_celonis')
    def test_data_model_permission_error(self, mock_get_celonis):
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance
        
        mock_data_pool = MagicMock()
        mock_celonis_instance.data_integration.get_data_pool.return_value = mock_data_pool

        mock_data_pool.get_data_model.side_effect = PyCelonisPermissionError

        with self.assertRaises(PyCelonisPermissionError):
            CeloConnector(
                base_url=self.base_url,
                api_token=self.api_token,
                key_type=self.key_type,
                data_pool_id=self.data_pool_id,
                data_model_id=self.data_model_id,
                space_name=self.space_name,
                package_name=self.package_name,
                knowledge_model_name=self.knowledge_model_name
            )
    
    @patch('base.data_connector.get_celonis')
    def test_data_model_not_found_error(self, mock_get_celonis):
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance

        mock_data_pool = MagicMock()
        mock_celonis_instance.data_integration.get_data_pool.return_value = mock_data_pool

        mock_data_pool.get_data_model.side_effect = PyCelonisNotFoundError

        with self.assertRaises(PyCelonisNotFoundError):
            CeloConnector(
                base_url=self.base_url,
                api_token=self.api_token,
                key_type=self.key_type,
                data_pool_id=self.data_pool_id,
                data_model_id=self.data_model_id,
                space_name=self.space_name,
                package_name=self.package_name,
                knowledge_model_name=self.knowledge_model_name
            )

    @patch('base.data_connector.get_celonis')
    def test_sapce_package_knowledge_model_permission_error(self, mock_get_celonis):
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance

        mock_space = MagicMock()        
        mock_celonis_instance.studio.get_spaces.return_value.find.return_value = mock_space
        mock_space.get_packages.return_value.find.side_effect = PyCelonisPermissionError
        

        with self.assertRaises(PyCelonisPermissionError):
            CeloConnector(
                base_url=self.base_url,
                api_token=self.api_token,
                key_type=self.key_type,
                data_pool_id=self.data_pool_id,
                data_model_id=self.data_model_id,
                space_name=self.space_name,
                package_name=self.package_name,
                knowledge_model_name=self.knowledge_model_name
            )
    
    @patch('base.data_connector.get_celonis')
    def test_sapce_package_knowledge_model_not_found_error(self, mock_get_celonis): 
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance

        mock_space = MagicMock()        
        mock_celonis_instance.studio.get_spaces.return_value.find.return_value = mock_space
        mock_space.get_packages.return_value.find.side_effect = PyCelonisNotFoundError

        with self.assertRaises(PyCelonisNotFoundError):
            CeloConnector(
                base_url=self.base_url,
                api_token=self.api_token,
                key_type=self.key_type,
                data_pool_id=self.data_pool_id,
                data_model_id=self.data_model_id,
                space_name=self.space_name,
                package_name=self.package_name,
                knowledge_model_name=self.knowledge_model_name
            )

    def test_create_pql_query_field(self):
        """
        tests the helper function that transforms field objects 
        into pql columns
        """
        fields = [
            Field(table='o_celonis_SalesOrder', column='NetAmount'),
            Field(table='o_celonis_SalesOrder', column='DocumentType'),
            Field(table='o_celonis_SalesOrder', column='SalesOrg'),
        ]
        query = create_pql_query(fields)
 
        self.assertIsInstance(query, pql.PQL)
        self.assertEqual(len(query.columns), 3)
    
    def test_create_pql_query_fields_and_filter(self):
        fields = [
            Field(table='o_celonis_SalesOrder', column='NetAmount'),
            Field(table='o_celonis_SalesOrder', column='DocumentType'),
            Field(table='o_celonis_SalesOrder', column='SalesOrg'),
        ]
        filt = Filter(pql='Filter "o_celonis_SalesOrder"."Status" = \'open\';')
        query = create_pql_query(fields, filt)
        self.assertIsInstance(query, pql.PQL)
        self.assertEqual(len(query.filters), 1)

    @patch('base.data_connector.get_celonis')
    @patch('base.data_connector.KnowledgeModelSaolaConnector')
    def test_run_query_no_filter(self, mock_get_celonis, MockKnowledgeModelSaolaConnector):
        mock_celonis_instance = MagicMock()
        mock_get_celonis.return_value = mock_celonis_instance

        celo_connetctor = CeloConnector(
            base_url=self.base_url,
            api_token=self.api_token,
            key_type=self.key_type,
            data_pool_id=self.data_pool_id,
            data_model_id=self.data_model_id,
            space_name=self.space_name,
            package_name=self.package_name,
            knowledge_model_name=self.knowledge_model_name
        )

        fields = [
            Field(table='o_celonis_SalesOrder', column='NetAmount'),
            Field(table='o_celonis_SalesOrder', column='DocumentType'),
            Field(table='o_celonis_SalesOrder', column='SalesOrg'),
        ]

        pql_df = celo_connetctor.run_query(fields)

        self.assertIsInstance(pql_df, pql.DataFrame)
        self.assertEqual(len(pql_df.columns), 3)
        self.assertTrue('NetAmount' in pql_df.columns)




