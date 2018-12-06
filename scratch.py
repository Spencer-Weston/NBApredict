from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import inspect
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base



class LinearRegressionSK:
    """linear regression class built with sci-kit learn. Replaced in ff_regression w/ statsmodels"""

    def __init__(self, target, predictors):
        self.target = target
        self.predictors = predictors
        lm = linear_model.LinearRegression()
        self.reg = lm.fit(predictors, target)
        # coefs = pd.DataFrame(zip(predictors.columns, lm.coef_), columns=["features", "estimated_coefs"])
        self.predictions = self.reg.predict(predictors)
        self.r_squared = self.reg.score(predictors, target)
        self.r_squared_rnd = np.around(self.r_squared, 3)
        self.residuals = self.target - self.predictions
        coefs= pd.DataFrame(lm.coef_, index=predictors.columns, columns =["estimated_coefs"])

    def predicted_vs_actual(self, out_path=None):
        graphing.pred_vs_actual(self.predictions, self.target, self.r_squared_rnd, out_path=out_path)