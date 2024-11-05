from sklearn.base import BaseEstimator, TransformerMixin

class DateTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(Self, X, y = None):
        X["year"] = X.date.dt.year
        X["month"] = X.date.dt.month
        X["day"] = X.date.dt.day
        X["dow"] = X.date.dt.dayofweek
        X["quarter"] = X.date.dt.quarter
        X = X.drop("date", axis=1)
        X = X.astype(str)
        return X