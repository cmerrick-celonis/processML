from sklearn.base import BaseEstimator
from typing import List

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
    model: the sklearn model you want to use to learn from the data
    target: the variable we are trying to predict - written in PQL
    predictors: the set of variables to use of predictors for the target - written in PQL format

    """

    def __init__(self, name:str, base_url:str, api_token:str, key_type:str, model:BaseEstimator,
                 target:str, predictors:List[str]):
        self.name = name
        self.connector = None #CeloConnector(base_url, api_token, )
        self.model = None #ModelTrainer(model)
        self.target = target
        self.predictors = predictors
        self.writer = None
        
    
    def extract_data(self):
        pass

    def preprocess_data(self):
        raise NotImplemented('This method should be implemented in the child class')
    
    def train_model(self):
        pass

    def write_predictions(self):
        pass



