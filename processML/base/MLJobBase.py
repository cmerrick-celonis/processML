from sklearn.base import BaseEstimator
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from typing import List, Optional
from data_connector import CeloConnector
from data_model import Field, Filter
from preprocessors import BasePreprocessor
from pandas import DataFrame

class MLJobBase():
    """
    The base class representing a machine learning job defined with processML
    in Celonis. Designed to allow customizable configuration.

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
    predictors: the set of variables to use of predictors for the target - written in PQL.
    training_objects_filter: a filter that selects the objects that are relevant for training a model. For example, when predicting
    delivery date, orders that are not delivered are not relevant. Objects that return false agaisnt this filter are the ones
    we want to predict values for!
    """

    #Configurable Class Attributes
    name: str
    model: BaseEstimator
    target: Field
    predictors: List[Field]
    training_objects_filter: Optional[Filter]
    preprocessor: Optional[BasePreprocessor]
    param_grid: Optional[dict]
    
    
    def __init__(self, base_url:str, api_token:str, key_type:str, data_pool_id:str, data_model_id:str, space_name:str,
                 package_name:str, knowledge_model_name:str):
        
        self.celonis_connection = CeloConnector(base_url, api_token, key_type, data_model_id, data_pool_id, space_name, package_name, knowledge_model_name)

    def extract_data(self, filter:Optional[Filter]) -> DataFrame:
        """
        extracts the data for the target and the predictors as a pandas dataframe. 

        params:
        ------
        filter: pass an optional pql filter to the data
        """
        if self.predictors:
            fields = self.predictors + self.target
        else:
            fields = [self.target]
        
        pql_dataframe = self.celonis_connection.run_query(fields, filter)

        return pql_dataframe.to_pandas()

    def preprocess_data(self, data:DataFrame) -> DataFrame:
        """
        uses the preprocessor defined on class to preprocess that data
        """
        if self.preprocessor is None:
            return
        
        return self.preprocessor.transform(data)
        

        

        
    def train_model(self, data:DataFrame) -> BaseEstimator:
        """
        fits the model to the data 
        """
        target_data, predictors_data = data[self.target.column], data[[i.column for i in self.predictors]]
        X_train, X_test, y_train, y_test = train_test_split(predictors_data, target_data)

        self.model.fit(X_train, y_train)

        self.evaluate_model(X_test, y_test)

    def evaluate_model(self) -> dict:
        pass

    def write_predictions(self):
        pass

    
