"""
Base class for connecting to a celonis instance with simplified methods for extraction and upload
"""

from pycelonis import get_celonis
from pycelonis_core.utils.errors import PyCelonisPermissionError, PyCelonisNotFoundError
import pycelonis.pql as pql
from pycelonis.pql.saola_connector import KnowledgeModelSaolaConnector
from pandas import DataFrame
from .data_model import Field, Filter
from typing import List, Optional
import logging
from utils.data_extraction import create_pql_query
from utils.data_upload import create_column_config_from_dataframe

class CeloConnector():
    """
    Responsible for connecting to a celonis instance, extracting tables based on user defined 
    queries and writing predictions back to the data model.

    For help on connecting to celonis with python see: 
    https://celonis.github.io/pycelonis/2.8.0/tutorials/executed/01_quickstart/02_celonis_basics/#12-connect-to-the-ems

    params
    ------
    base_url: the url of the celonis instance you are trying to connect to
    api_token: an api token with the correct permission for the instance you are connecting to
    key_type: a user key or an app key
    data_pool_id: the id of the data pool 
    data_model_id: the id of the data model
    space_name: the name of the space that contains the knowledge model
    package_name: the name of the package that contains the knowledge model
    knowledge_model_name: the name of the knowledge model in the package
    """

    def __init__(self, base_url:str, api_token:str, key_type:str, data_pool_id:str, data_model_id:str, space_name:str, 
                 package_name:str, knowledge_model_name:str):
        celonis = get_celonis(base_url, api_token, key_type)
        try:
            self.data_pool = celonis.data_integration.get_data_pool(data_pool_id)
        except PyCelonisPermissionError:
            logging.error(f'Insufficient permissions to access data pool - see Admin and Permissions in your celonis instance')
            raise
        except:
            PyCelonisNotFoundError(f'Data pool with id {data_pool_id} cannot be found in the celonis instance.') 
            raise

        try:
            self.data_model = self.data_pool.get_data_model(data_model_id)
        except PyCelonisPermissionError:
            logging.error(f'Insufficient permissions to access data pool - see Admin and Permissions in your celonis instance')
            raise 
        except:
            PyCelonisNotFoundError(f'Data model with id {data_model_id} cannot be found in the celonis instance.') 
            raise
        
        try:
            self.knowledge_model = celonis.studio.get_spaces().find(space_name).get_packages().find(package_name).get_knowledge_models().find(knowledge_model_name)
        except PyCelonisPermissionError:
            logging.error(f'Insufficient permissions to access spaces and packages - see Admin and Permissions in your celonis instance')
            raise 
        except PyCelonisNotFoundError:
            logging.error(f'Space, package or knowledge name invalid.')
            raise
        
        self.saola_connector = KnowledgeModelSaolaConnector(self.data_model, self.knowledge_model)


    
    def run_query(self, fields:List[Field], filter:Optional[Filter]=None)->pql.DataFrame:
        """
        executes a PQL query string agaisnt the loaded data model to extracts data from the ems

        params
        ------
        fields: the list of columns to extract
        filter: a filter for the extract

        returns
        -------
        pql_df: a PQL dataframe containing the data extract.
        """
        query = create_pql_query(fields, filter)
        pql_df = pql.DataFrame.from_pql(query=query, saola_connector=self.saola_connector)
        return pql_df
        

    def write_data(self, df:DataFrame, name:str):
        """
        write the data in data into the data model table specified under the column heading name.
        This works by first writing the dataframe into the data pool and then from the data pool 
        into the data model

        params
        ------
        df: the dataframe to write into the data model
        name: the name the column will have in the data model
        data_model_table_name: the name of the data_model_table in the model
        """
        column_config = create_column_config_from_dataframe(df)
        new_data_pool_table = self.data_pool.create_table(df=df, 
                                                          table_name=name, 
                                                          drop_if_exists=True,
                                                          column_config=column_config)
        
        new_data_model_table = self.data_model.add_table(new_data_pool_table)
        #TODO: create foreign key to object table
        #TODO: check if table exists
        #TODO: refresh data model.

        

        



    