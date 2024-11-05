from sklearn.base import BaseEstimator
from typing import List
from data_connector import CeloConnector
from data_model import Field
from pandas import DataFrame

class MLJobBase():
    """
    The base class representing a machine learning job defined with processML
    in Celonis. An ML job should know how to extract data from celonis, preprocess
    this data, train an sklearn classifier and write any predictions back into 
    the data model in celonis.

    For help on connecting to celonis with python see: 
    https://celonis.github.io/pycelonis/2.8.0/tutorials/executed/01_quickstart/02_celonis_basics/#12-connect-to-the-ems

    params
    ------
    name: the name of the ml job 
    base_url: the url of the celonis instance your are trying to connect to
    api_token: an api token with the correct permission for the instance you are connecting to
    key_type: a user key or an app key
    data_pool_id: the id of the data pool 
    data_model_id: the id of the data model
    space_name: the name of the space that contains the knowledge model
    package_name: the name of the package that contains the knowledge model
    knowledge_model_name: the name of the knowledge model in the package
    model: the sklearn model 
    target: the variable we are trying to predict - written in PQL
    predictors: the set of variables to use of predictors for the target - written in PQL
    """
    
    def __init__(self, name:str, base_url:str, api_token:str, key_type:str, data_pool_id:str, data_model_id:str, space_name:str,
                 package_name:str, knowledge_model_name:str, model:BaseEstimator, target:Field, predictors:List[Field]):
        
        self.name = name
        self.celonis_connection = CeloConnector(base_url, api_token, key_type, data_model_id, data_pool_id, space_name, package_name, knowledge_model_name)
        self.model = None #ModelTrainer(model)
        self.target = target
        self.predictors = predictors
        
    
    def extract_data(self) -> DataFrame:
        """
        extracts the data for the target and the predictors as a pandas dataframe. 
        """
        pass
        

    def preprocess_data(self):
        raise NotImplemented('This method should be implemented in the child class')
    
    def train_model(self):
        pass

    def write_predictions(self):
        pass

    


