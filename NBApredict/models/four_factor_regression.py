"""
Author: Spencer Weston

Purpose: Four Factor Regression performs a regression on the Margin of Victory (mov) between NBA teams with each teams
four factors(offensive and defensive) as predictors. The regression object is returned from the module.

Args (default):
    year (2019): The year of the season desired
    db_url ('sqlite:///database//nba_db.db'): Path to the database where data should be written

Returns:
    Returns a LinearRegression class
"""
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import scipy.stats as stats
from sqlalchemy.orm import Session
from sqlalchemy import func, alias

import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif

# Local Packages
from datatotable.database import Database
from nbapredict.database import getters
from nbapredict.helpers import br_references as br
from nbapredict.management import conversion
from nbapredict.models import graphing
from nbapredict.configuration import Config


class LinearRegression:
    """A class that creates and holds linear regression information and functions for regression evaluation.

    LinearRegression is initialized with a target variable and the desired predictors. Then, a regression is run and
    necessary regression stats are stored as class parameters. Member functions generate evaluative graphs and/or
    stats for the regression.

    Attributes:
        target: The target variable
        predictors: The predictive variables
        results: statsmodels results wrapper
        predictions: predicted results from the regression
        r_squared: r_squared of the regression
        adj_r_squared: adj_r_squared of the regression
        r_squared_rnd: r_squared rounded to three decimal places
        residuals: residuals of the gression
        p_values: p_values of the coefficients
        coefs: values of the coefficients
        output: data frame of coefficients with their values and p_values"""

    def __init__(self, target, predictors):
        """Performs a linear regression and stores pertinent regression outputs as class variables

        Args:
            target: The target variable
            predictors: The prediction variables"""
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
        """Generate a predicted vs. actual graph, save to out_path if it exists, and return the graph."""
        graph = graphing.pred_vs_actual(self.predictions, self.target, self.r_squared_rnd, out_path=out_path)
        return graph

    def residuals_vs_fitted(self, out_path=None):
        """Generate a residuals vs. fitted graph, save to out_path if it exists, and return the graph."""
        graph = graphing.residuals_vs_fitted(self.predictions, self.residuals, out_path)
        return graph

    def qqplot(self, out_path=None):
        """Generate a qq plot, save to out_path if it exists, and return the graph."""
        fig = sm.qqplot(self.residuals, dist=stats.t, fit=True, line="45")
        if out_path:
            fig.savefig(out_path)
        return fig

    def influence_plot(self, out_path=None):
        """Generate an influence plot, save to out_path if it exists, and return the graph."""
        fig, ax = plt.subplots(figsize=(12, 8))
        fig = sm.graphics.influence_plot(self.results, alpha=0, ax=ax, criterion="cooks")
        if out_path:
            fig.savefig(out_path)
        return fig

    def cooks_distance(self, out_path=None):
        """Generate a cook's distance graph, save to out_path if it exists, and return the graph."""
        influence = self.results.get_influence()
        # c is the distance and p is p-value
        (c, p) = influence.cooks_distance
        graph = graphing.cooks_distance(c, out_path)
        return graph

    def residual_independence(self, out_path=None):
        """Generate a residual independence plot, save to out_path if it exists, and return the graph."""
        residuals = self.residuals
        plot = graphing.residual_independence(residuals)
        if out_path:
            plot.savefig(out_path)
        return plot

    def vif(self):
        """Determine the Variance Inflation Factor (vif) of the coefficients and return a dataframe of the vif's."""
        vif_out = pd.DataFrame()
        predictors = np.array(self.predictors)
        vif_out["VIF Factor"] = [vif(predictors, i) for i in range(predictors.shape[1])]
        vif_out["features"] = self.predictors.columns
        return vif_out

    def residual_distribution(self):
        """Calculate the normal curve of the residuals and return the distribution"""
        norm = stats.norm
        mu, std = norm.fit(self.residuals)
        # mu = 0  # By definition, mu of resids = 0, but the fit provides approximately 0. It's perhaps best to just
        # set mu=0?
        return norm(loc=mu, scale=std)


def create_ff_regression_df(session, team_stats_tbl, sched_tbl, ff_list):
    """Create and return a regression data frame of the four factors (ff) for each team in a matchup.

    Args:
        session: Sqlalchemy session object
        team_stats_tbl: mapped team stats table object
        sched_tbl: mapped schedule table object
        ff_list: List of the four factors variable

    Returns:
         A data frame with home('_h') and away('_a') stats and the margin of victory (mov). The mov is the target
         for a regression. The '_h' and '_a" stats are the home and away four factors in a specific matchup.
    """
    home_stats = alias(team_stats_tbl, name='home')
    away_stats = alias(team_stats_tbl, name='away')
    sched = alias(sched_tbl, name='sched')
    home_stat_ff = [getattr(home_stats.c, col) for col in ff_list if col in home_stats.c.keys()]
    away_stat_ff = [getattr(away_stats.c, col) for col in ff_list if col in away_stats.c.keys()]
    sched_stats_query = session.query(sched, *home_stat_ff, *away_stat_ff).filter(sched.c['home_team_score'] > 0).\
        join(home_stats, home_stats.c['id'] == sched.c['home_stats_id']).\
        join(away_stats, away_stats.c['id'] == sched.c['away_stats_id']).subquery(with_labels=True)
    sched_stats = session.query(sched_stats_query)

    df = conversion.convert_sql_statement_to_table(session, sched_stats.statement)
    return df


def alt_regression_df(session, team_stats_tbl, sched_tbl, ff_list):
    """Alternate regression df where the latest team_stats are applied to all games in schedule"""
    team_stats = session.query(team_stats_tbl).group_by(team_stats_tbl.team_id).having(func.max(team_stats_tbl.id)).\
        subquery()
    home_stats = alias(team_stats, name='home')
    away_stats = alias(team_stats, name='away')
    sched = alias(sched_tbl, name='sched')
    home_stat_ff = [getattr(home_stats.c, col) for col in ff_list if col in home_stats.c.keys()]
    away_stat_ff = [getattr(away_stats.c, col) for col in ff_list if col in away_stats.c.keys()]

    sched_stats_query = session.query(sched, *home_stat_ff, *away_stat_ff).filter(sched.c['home_team_score'] > 0).\
        join(home_stats, home_stats.c['team_id'] == sched.c['home_team_id']).\
        join(away_stats, away_stats.c['team_id'] == sched.c['away_team_id']).subquery(with_labels=True)

    sched_stats = session.query(sched_stats_query)

    df = conversion.convert_sql_statement_to_table(session, sched_stats.statement)
    return(df)

def get_team_ff(ff_df, team, ff_list, home):
    """Extract the four factors for a specific team from the ff_df and return the result.

    Further, if home is True, a "_h" is appended to each four factor for the team. And if False, "_a" is appended.
    This is to specify if the team is home or away.

    Args:
        ff_df: four factors Pandas data frame (read from SQL table)
        team: A team name
        ff_list: List of the four factors variable
        home: Boolean. True if the team is home; False if the team is away
    """
    team_ff = ff_df[ff_df.team_name.str.lower() == team.lower()][ff_list]
    if home:
        team_ff = team_ff.rename(append_h, axis='columns')
    else:
        team_ff = team_ff.rename(append_a, axis='columns')
    return team_ff


def append_h(string):
    """Append "_h" to string and return the modified string"""
    string = '{}{}'.format(string, '_h')
    return string


def append_a(string):
    """Append "_a" to string and return the modified string"""
    string = '{}{}'.format(string, '_a')
    return string


def ensure_unique_index(index, indices, i=1):  # Indexed to 1 so +1 == 2nd, 3rd, 4th, etc. game
    """Check if index is in indices, modify index until it's unique, and return the unique index

    If the index is unique, it's returned as is. Otherwise, the function calls itself and increments i. The recursion
    stops when the index and numerical suffix (i) are not in indices. Used to create unique identifiers for multiple
    matchups between the same teams.

    Args:
        index: A string index to check for in indices
        indices: A list of indices to check the index against
        i: A numerical suffix used to modify index until it does not exist in indices
    Returns:
        index, or a modified form of index, that does not exist in indices
    """
    if index in indices:
        i = i+1
        test_index = "{}{}".format(index, i)
        if test_index in indices:
            return ensure_unique_index(index, indices, i)
        else:
            return test_index
    else:
        return index


def four_factors_list():
    """Create a four factor(ff) list and identifying information and return it."""
    # Import and specify a list of factors to extract from database
    ff_list = br.four_factors.copy()
    return ff_list


def main(session, graph=False):
    """Create a regression data frame, run a regression through the LinearRegression class, and return the class

    Args:
        database: An instantiated DBInterface object from dbinterface.py
        session: An instantiated Session object from sqlalchemy
        graph: A boolean that creates graphs if true

    Returns:
        A LinearRegression class
    """
    league_year = Config.get_property("league_year")
    graph_dir = Config.get_property("graph_dir")
    if not os.path.exists(graph_dir) and graph:
        os.mkdir(graph_dir)

    # Import and specify a list of factors to extract from database
    ff_list = four_factors_list()

    # Convert database tables to pandas
    team_stats_tbl = db.table_mappings['team_stats_{}'.format(league_year)]
    sched_tbl = db.table_mappings['schedule_{}'.format(league_year)]
    #regression_df = create_ff_regression_df(session, team_stats_tbl, sched_tbl, ff_list)
    regression_df = alt_regression_df(session, team_stats_tbl, sched_tbl, ff_list)

    # Separate DF's into them into X (predictors) and y (target)
    predictors = regression_df[regression_df.columns.drop(list(regression_df.filter(regex='sched')))]
    target = regression_df["sched_MOV"]

    ff_reg = LinearRegression(target, predictors)

    # Note: On Windows, graphs will not appear to update
    # To change that, go to folder properties -> customize -> optimize for: Documents
    if Config.get_property("graph"):
        ff_reg.predicted_vs_actual(out_path=os.path.join(graph_dir, "pred_vs_actual_{}.png".format(league_year)))
        ff_reg.residuals_vs_fitted(out_path=os.path.join(graph_dir, "residuals_vs_fitted_{}.png".format(league_year)))
        ff_reg.qqplot(out_path=os.path.join(graph_dir, "qqplot_{}.png".format(league_year)))
        ff_reg.influence_plot(out_path=os.path.join(graph_dir, "influence_{}.png".format(league_year)))
        ff_reg.cooks_distance(out_path=os.path.join(graph_dir, "cooks_distance_{}.png".format(league_year)))
        ff_reg.residual_independence(out_path=os.path.join(graph_dir, "resid_independence_{}.png".format(league_year)))

    # Multicollinearity
    # vif_df = ff_reg.vif()
    ff_reg.residual_distribution()

    return ff_reg


if __name__ == "__main__":
    db = Database('test', "../management")
    session = Session(db.engine)
    test = main(session, graph=True)
    t=2
