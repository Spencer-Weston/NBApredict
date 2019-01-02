import br_references as cd
import graphing
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif
import scipy.stats as stats


class LinearRegression:

    def __init__(self, target, predictors):
        """Performs a linear regression and stores pertinent regression outputs as class variables"""
        self.target = target
        self.predictors = sm.add_constant(predictors)
        self.results = sm.OLS(target, self.predictors).fit()
        self.predictions = self.results.predict(self.predictors)
        self.r_squared = self.results.rsquared
        self.adj_r_squared = self.results.rsquared_adj
        self.r_squared_rnd = np.around(self.r_squared, 3)
        self.residuals = self.results.resid
        self.p_values = self.results.pvalues
        self.coefs = self.results.params
        self.output = pd.concat([self.coefs, self.p_values], axis=1)
        self.output.columns = ["coefficient", "p_value"]

    def predicted_vs_actual(self, out_path=None):
        graph = graphing.pred_vs_actual(self.predictions, self.target, self.r_squared_rnd, out_path=out_path)
        return graph

    def residuals_vs_fitted(self, out_path=None):
        graph = graphing.residuals_vs_fitted(self.predictions, self.residuals, out_path)
        return graph

    def qqplot(self, out_path=None):
        fig = sm.qqplot(self.residuals, dist=stats.t, fit=True, line="45")
        if out_path:
            fig.savefig(out_path)
        return fig

    def influence_plot(self, out_path=None):
        fig, ax = plt.subplots(figsize=(12, 8))
        fig = sm.graphics.influence_plot(self.results, ax=ax, criterion="cooks")
        if out_path:
            fig.savefig(out_path)
        return fig

    def cooks_distance(self, out_path=None):
        influence = self.results.get_influence()
        # c is the distance and p is p-value
        (c, p) = influence.cooks_distance
        graph = graphing.cooks_distance(c, out_path)
        return graph

    def vif(self):
        """Variance Inflation Factor"""
        vif_out = pd.DataFrame()
        predictors = np.array(self.predictors)
        vif_out["VIF Factor"] = [vif(predictors, i) for i in range(predictors.shape[1])]
        vif_out["features"] = self.predictors.columns
        return vif_out

    def residual_distribution(self):
        """Calculates the normal curve of the residuals"""
        norm = stats.norm
        mu, std = norm.fit(self.residuals)
        mu = 0  # By definition, mu of resids = 0, but the fit provides approximately 0. It's perhaps best to just
        # set mu=0?
        return norm(loc=mu, scale=std)


def create_ff_regression_df(ff_df, sched_df, ff_list):
    """ Pd.concat presents a performance issue

    ff_df: four factors Pandas dataframe
    sched_df: Schedule dataframe
    ff_list: List of the four factors variable
    return: a dataframe with home('_h') and away('_a') statistics and the margin of victory
    """
    initialized_df = False
    for index, row in sched_df.iterrows():
        home_tm = row["home_team"]
        away_tm = row["away_team"]
        mov = row["home_team_score"] - row["away_team_score"]

        home_tm_ff = get_team_ff(ff_df, home_tm, ff_list, home=True)
        home_tm_ff["key"] = 1
        home_tm_ff["mov"] = mov
        away_tm_ff = get_team_ff(ff_df, away_tm, ff_list, home=False)
        away_tm_ff["key"] = 1

        merged = pd.merge(home_tm_ff, away_tm_ff, on="key")
        if not initialized_df:
            regression_df = merged
            initialized_df = True
        else:
            regression_df = pd.concat([regression_df, merged], sort=True)
    regression_df = regression_df.drop(["key"], axis=1)

    return regression_df


def get_team_ff(ff_df, team, ff_list, home):
    team_ff = ff_df[ff_df.team_name.str.lower() == team.lower()][ff_list]
    if home:
        team_ff = team_ff.rename(append_h, axis='columns')
    else:
        team_ff = team_ff.rename(append_a, axis='columns')
    return team_ff


def append_h(string):
    string = '{}{}'.format(string, '_h')
    return string


def append_a(string):
    string = '{}{}'.format(string, '_a')
    return string


def four_factors_list():
    # Import and specify a list of factors to extract from database
    ff_list = cd.four_factors.copy()

    ff_list.insert(0, "team_name")
    ff_list.append("wins")
    ff_list.append("losses")
    ff_list.append("mov")
    return ff_list


def main(year=2019, graph=False):
    # Variable setup
    db_url = "sqlite:///database//nba_db.db"
    engine = create_engine(db_url)
    conn = engine.connect()

    # Import and specify a list of factors to extract from database
    ff_list = four_factors_list()

    # Database table to pandas table
    misc_stats = "misc_stats_{}".format(year)
    sched = "sched_{}".format(year)
    ff_df = pd.read_sql_table(misc_stats, conn)[ff_list]  # FF = four factors
    sched_df = pd.read_sql_table(sched, conn)

    # Combines four factors and seasons df's and separates them into X and y
    regression_df = create_ff_regression_df(ff_df, sched_df, cd.four_factors)
    predictors = regression_df.loc[:, regression_df.columns != 'mov']
    target = regression_df["mov"]

    ff_reg = LinearRegression(target, predictors)

    # Evaluative graphs
    if graph:
        ff_reg.predicted_vs_actual(out_path=r"graphs/pred_vs_actual_{}.png".format(year))
        ff_reg.residuals_vs_fitted(out_path=r"graphs/residuals_vs_fitted_{}.png".format(year))
        ff_reg.qqplot(out_path=r"graphs/qqplot_{}.png".format(year))
        ff_reg.influence_plot(out_path=r"graphs/influence_{}.png".format(year))
        ff_reg.cooks_distance(out_path=r"graphs/cooks_distance_{}.png".format(year))


    # Multicollinearity
    # vif_df = ff_reg.vif()
    ff_reg.residual_distribution()

    return ff_reg
    print("FINISHED")


if __name__ == "__main__":
    main(year=2019, graph=True)

