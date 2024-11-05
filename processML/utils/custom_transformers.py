from sklearn.base import BaseEstimator, TransformerMixin
from pandas import DataFrame, Series

class DateTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X:DataFrame, y=None):
        """
        extracts date features from dataframe passed
        """
        X = X.copy()
        
        if len(X.columns) == 1:
            X: Series = X.iloc[:, 0]
        
        Features = DataFrame(index=range(len(X)))
        Features["year"] = X.dt.year
        Features["month"] = X.dt.month
        Features["day"] = X.dt.day
        Features["dow"] = X.dt.dayofweek
        Features["quarter"] = X.dt.quarter
        Features["hour"] = X.dt.hour
        Features["minute"] = X.dt.minute
        return Features