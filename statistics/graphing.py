"""
Author: Spencer Weston

Purpose: Contains functions for creating evaluative graphs for regressions
"""

import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
from statsmodels.compat import lzip
from yellowbrick.regressor import ResidualsPlot


def pred_vs_actual(predictions, target, r_squared, out_path=None):
    """Create and returnsa scatter plot of a model's predictions versus target variables

    Args:
        predictions: The predictions from a regression
        target: The target variable of a regression
        r_squared: The r_squared of a regression
        out_path: An optional path to save the graph to
    Returns:
        The predicted vs. actual graph
        """

    # Generate coordinates for a 1:1 line
    minimum = int(predictions.min()) - 1
    maximum = int(predictions.max()) + 1
    diag_line_x = [i for i in range(minimum, maximum)]
    diag_line_y = [i for i in diag_line_x]

    # Build Scatterplot
    fig, ax = plt.subplots()
    ax.scatter(predictions, target)
    ax.set_title("Predicted vs. Actual")
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    ax.axhline(0, c="k", linewidth=0.25)
    ax.plot(diag_line_x, diag_line_y, c="r")
    ax.text(0.1, 0.9, "R^2 = {}".format(r_squared), transform=ax.transAxes, bbox=dict(fill=False))

    if out_path:
        fig.savefig(out_path)
    return fig


def residuals_vs_fitted(predictions, residuals, out_path=None):
    """Create and return a scatter plot of a model's fitted values (predictions) versus the residuals

    Args:
        predictions: The predictions from a regression
        residuals: The residuals from a regression
        out_path: An optional path to save the graph to

    Returns:
        The residuals vs. fitted graph
    """
    # Get Jarque-bera test of normality
    name = ['Jarque-Bera', 'Chi^2 two-tail prob.', 'Skew', 'Kurtosis']
    test = sm.stats.jarque_bera(residuals)
    jarque_bera = lzip(name, test)
    p_value = jarque_bera[1][1]

    # Build Scatterplot
    fig, ax = plt.subplots(nrows=1, ncols=2, gridspec_kw={'width_ratios': [3, 1]})
    ax[0].scatter(predictions, residuals)
    ax[0].set_title("Residuals vs. Fitted Values")
    ax[0].set_xlabel("Fitted Values")
    ax[0].set_ylabel("Residuals")
    ax[0].axhline(0, c="k", linewidth=0.5)
    ax[1].hist(residuals, bins=30, orientation="horizontal")
    ax[1].set_xlabel("Frequency")
    ax[1].set_title("Jarque-Bera \n P-Value: {}".format(np.around(p_value, 3)))

    if out_path:
        fig.savefig(out_path)
    return fig


def cooks_distance(cooks_d, out_path):
    """Create and return a cook's distance graph

    Args:
        cooks_d: Cook's distance from a regression

    Returns:
        The cook's distance graph
    """
    fig, ax = plt.subplots()
    ax.stem(np.arange(len(cooks_d)), cooks_d)
    ax.set_title("Cook's Distance")
    ax.set_xlabel("Residuals")
    ax.set_ylabel("Cook's Distance")
    if out_path:
        fig.savefig(out_path)
    return fig


def residuals_yellowbrick(predictors, target):
    """Returns a residuals vs. fitted graph with a histogram. Not currently functional.

    For future development. uses yellowbrick, which makes good graphs, but experiencing an unexplained missing
    argument TypeError
    """
    lm = LinearRegression
    visualizer = ResidualsPlot(lm)
    visualizer.fit(predictors, target)
    return visualizer
