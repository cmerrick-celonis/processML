import unittest
import datetime as dt
from pandas import DataFrame, NA
from base.preprocessors import BasePreprocessor, BasicPreprocessor

class TestBasePreprocessor(unittest.TestCase):
    def setUp(self):
        self.full_data = DataFrame( data={
            'Amount':[100, 200, 300, 400, 500],
            'Date':[dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 2), dt.datetime(2024, 10, 3)],
            'DocumentType':['RV', 'RV', 'RV', 'ZX', 'ZX'],
            'OpenOrder':[True, False, False, False, True]
            })
        
        self.partial_data = DataFrame(data={
            'Amount':[100, 200, NA, 400, 500],
            'Date':[dt.datetime(2024, 10, 1), NA, dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 2), dt.datetime(2024, 10, 3)],
            'DocumentType':['RV', 'RV', 'RV', NA, NA],
            'OpenOrder':[NA, False, False, NA, True]
            })
        return super().setUp()
    
    def test_find_column_types_full_data(self):
        """
        checks type inference is working
        """
        preprocessor = BasePreprocessor()
        preprocessor._find_column_types(self.full_data)
        self.assertEqual(len(preprocessor.column_types['numeric']), 1)
        self.assertEqual(len(preprocessor.column_types['categorical']), 2)
        self.assertEqual(len(preprocessor.column_types['date']), 1)
    
    def test_find_column_types_partial_data(self):
        "checks type inference is working on partial data"
        preprocessor = BasePreprocessor()
        preprocessor._find_column_types(self.partial_data)
        self.assertEqual(len(preprocessor.column_types['numeric']), 1)
        self.assertEqual(len(preprocessor.column_types['categorical']), 2)
        self.assertEqual(len(preprocessor.column_types['date']), 1)


class TestBasicPreprocessor(unittest.TestCase):

    def setUp(self):
        self.full_data = DataFrame( data={
            'Amount':[100, 200, 300, 400, 500],
            'Date':[dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 2), dt.datetime(2024, 10, 3)],
            'DocumentType':['RV', 'RV', 'RV', 'ZX', 'ZX'],
            'OpenOrder':[True, False, False, False, True]
            })
        
        self.partial_data = DataFrame(data={
            'Amount':[100, 200, NA, 400, 500],
            'Date':[dt.datetime(2024, 10, 1), NA, dt.datetime(2024, 10, 1), dt.datetime(2024, 10, 2), dt.datetime(2024, 10, 3)],
            'DocumentType':['RV', 'RV', 'RV', NA, NA],
            'OpenOrder':[NA, False, False, NA, True]
            })
        return super().setUp()
    
    def test_build_pipeline(self):
        """
        ensures the class builds the pipeline correctly
        """
        preprocessor = BasicPreprocessor()
        preprocessor._find_column_types(self.full_data)
        preprocessor._build_pipeline(self.full_data)
        transformers = preprocessor.pipeline.transformers

        self.assertEqual(transformers[0][2], ['Amount'])
        self.assertEqual(transformers[1][2], ['DocumentType', 'OpenOrder'])
        self.assertEqual(transformers[2][2], ['Date'])


    def test_transform_full_data(self):
        preprocessor = BasicPreprocessor()
        transformed_data = preprocessor.transform(self.full_data)
        print(transformed_data)



    