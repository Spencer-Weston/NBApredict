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

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import scipy.stats as stats
from sqlalchemy.orm import Session
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif

# Local Packages
from database import getters
from database.database import Database
from helpers import br_references as br
from models import graphing
import configuration


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


def create_ff_regression_df(ff_df, sched_df, ff_list):
    """Create and return a regression data frame of the four factors (ff) for each team in a matchup.

    Pd.concat presents a performance issue

    Args:
        ff_df: four factors Pandas data frame (read from SQL table)
        sched_df: Schedule data frame (read from SQL table)
        ff_list: List of the four factors variable

    Returns:
         A data frame with home('_h') and away('_a') stats and the margin of victory (mov). The mov is the target
         for a regression. The '_h' and '_a" stats are the home and away four factors in a specific matchup.
    """
    initialized_df = False
    indices = []
    abbreviations = br.team_to_team_abbreviation
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

        # Creates a df index of team abbreviations and the game in series between teams
        # "BOS_WAS", "BOS_WAS2", "BOS_WAS3", etc.
        new_index = "{}_{}".format(abbreviations[home_tm], abbreviations[away_tm])
        new_index = ensure_unique_index(new_index, indices)

        # Sets the df index to the matchup and stores the value in new_indices to avoid duplicates
        merged["matchup"] = new_index
        merged.set_index("matchup", inplace=True)
        indices.append(new_index)

        # merged.reindex
        if not initialized_df:
            regression_df = merged.reindex([new_index])
            initialized_df = True
        else:
            regression_df = pd.concat([regression_df, merged], sort=True)

    # Create column list to put columns in correct order
    home_cols = home_tm_ff.drop(["key", "mov"], axis=1)
    away_cols = away_tm_ff.drop(["key"], axis=1)
    ordered_cols = ["mov", *home_cols.columns.to_list(), *away_cols.columns.to_list()]

    regression_df = regression_df.drop(["key"], axis=1)
    regression_df = regression_df[ordered_cols]


    return regression_df


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

    ff_list.insert(0, "team_name")
    ff_list.append("wins")
    ff_list.append("losses")
    ff_list.append("mov")
    return ff_list


def main(database, session, year=2019, graph=False):
    """Create a regression data frame, run a regression through the LinearRegression class, and return the class

    Args:
        database: An instantiated Database object from database.py
        session: An instantiated Session object from sqlalchemy
        year: The year to run the regression for
        graph: A boolean that creates graphs if true

    Returns:
        A LinearRegression class
    """
    graph_dir = configuration.graphs_directory()
    if not os.path.exists(graph_dir) and graph:
        os.mkdir(graph_dir)

    # Import and specify a list of factors to extract from database
    ff_list = four_factors_list()

    # Convert database tables to pandas
    ff_df = getters.get_pandas_df_from_table(database, session, "misc_stats_{}".format(year), ff_list)
    sched_df = getters.get_pandas_df_from_table(database, session, "sched_{}".format(year),
                                                lambda df: df.away_team_score > 0)

    # Combines four factors and seasons df's and separates them into X (predictors) and y (target)
    regression_df = create_ff_regression_df(ff_df, sched_df, br.four_factors)
    predictors = regression_df.loc[:, regression_df.columns != 'mov']
    target = regression_df["mov"]

    ff_reg = LinearRegression(target, predictors)

    # Note that on Windows graphs will not appear to be updated
    # To change that, go to folder properties -> customize -> optimize for: Documents
    if graph:
        ff_reg.predicted_vs_actual(out_path=os.path.join(graph_dir, "pred_vs_actual_{}.png".format(year)))
        ff_reg.residuals_vs_fitted(out_path=os.path.join(graph_dir, "residuals_vs_fitted_{}.png".format(year)))
        ff_reg.qqplot(out_path=os.path.join(graph_dir, "qqplot_{}.png".format(year)))
        ff_reg.influence_plot(out_path=os.path.join(graph_dir, "influence_{}.png".format(year)))
        ff_reg.cooks_distance(out_path=os.path.join(graph_dir, "cooks_distance_{}.png".format(year)))
        ff_reg.residual_independence(out_path=os.path.join(graph_dir, "resid_independence_{}.png".format(year)))

    # Multicollinearity
    # vif_df = ff_reg.vif()
    ff_reg.residual_distribution()

    return ff_reg


if __name__ == "__main__":
    database = Database()
    session = Session(database.engine)
    main(database, session, 2019, graph=True)
