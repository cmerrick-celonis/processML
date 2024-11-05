from sklearn import metrics
from sklearn.base import BaseEstimator

class BaseEvaluator():
    """
    Base evaluator. Subclass and define a metric as a class
    attribute to extend.
    """
    metric: function

    def evaluate(self, model:BaseEstimator, X_test, y_test) -> float:
        y_pred = model.predict(X_test)
        return self.metric(y_test, y_pred)

class MSEEvaluator(BaseEvaluator):
    """
    Uses MSE as the evaluation function
    """
    metric = metrics.mean_absolute_error

class AccuracyEvaluator(BaseEvaluator):
    """
    Uses accuracy score as the evaluation function
    """
    metric: metrics.accuracy_score

