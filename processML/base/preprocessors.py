import sklearn.preprocessing as preprocessing
import sklearn.impute as imputers
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from pandas import DataFrame
from pandas.api.types import infer_dtype
from utils.custom_transformers import DateTransformer

class BasePreprocessor():
    """
    Base class that implements most of the functionality required for a preprocessor.
    Subclasses need only implement a class to build the pipeline object
    """
    def __init__(self):
        self.pipeline = Pipeline(steps=[])
        self.column_types = {'categorical':[], 'numeric':[], 'date':[]}

    def _find_column_types(self, data:DataFrame):
        """
        assigns columns to a data type to assure preprocessing steps
        are correctly applied.

        if a column is not assigned a data type it is not transformed and therefore 
        is removed from the data

        params:
        ------
        data: the dataframe to assign column types to
        """
        for col in data.columns:
            inferred_dtype = infer_dtype(data[col], skipna=True)
            if inferred_dtype in ('integer', 'floating', 'mixed-integer-float', 'decimal'):
                self.column_types['numeric'].append(col)
            elif inferred_dtype in ('string', 'categorical', 'boolean'):
                self.column_types['categorical'].append(col)
            elif inferred_dtype in ('datetime', 'date', 'datetime64'):
                self.column_types['date'].append(col)
            else:
                continue

    def _build_pipeline(self):
        raise NotImplementedError('Implemented by subclasses')

    def transform(self, data: DataFrame):
        """
        transforms the data according to the pipeline built by 
        the preprocessor
        """
        self._find_column_types(data)
        self._build_pipeline(data)
        return self.pipeline.fit_transform(data)

class BasicPreprocessor(BasePreprocessor):
    """
    Does all the baisc preprocessing of numerical, categorical and date festures.
    Imputes missing values, scales numerical value and performs one hot encoding.
    """
    def __init__(self):
        super().__init__()

    def _build_pipeline(self, data:DataFrame):

        numeric_transformer = Pipeline([
            ('imputer', imputers.SimpleImputer(strategy='mean')),
            ('scaler', preprocessing.StandardScaler())
        ])

        categorical_transformer = Pipeline([
            ('encoder', preprocessing.OneHotEncoder(handle_unknown='ignore'))
        ])

        date_transformer = Pipeline([
            ('extract', DateTransformer())
        ])

        preprocessor = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, self.column_types['numeric']),
                ('cat', categorical_transformer, self.column_types['categorical']),
                ('date', date_transformer, self.column_types['date']),
            ]
        )

        self.pipeline = preprocessor

        




    

