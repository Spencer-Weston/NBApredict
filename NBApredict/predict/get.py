"""Funcs contains functions for generating predictions and their helper functions."""
import pandas as pd

import nbapredict.models.four_factor_regression as lm
import nbapredict.helpers.br_references as br_references
from nbapredict.database.manipulator import DataManipulator
import nbapredict.database.getters as getters


def sample_prediction(database, session, ref_tbl, model):
    """Generate and return a one row sample prediction created from the first row of the reference table.

    ToDo: Change function to take any model

    Args:
        database: An initialized DBInterface class from database.dbinterface.py
        session: A SQLalchemy session object
        ref_tbl: A mapped odds table
        model: A regression object from four_factor_regression.py

    Returns:
        A DataManipulator object initialized with a prediction from regression
    """
    first_game_odds = session.query(ref_tbl).order_by(ref_tbl.start_time).first()

    home_tm = first_game_odds.home_team
    away_tm = first_game_odds.away_team
    start_time = first_game_odds.start_time

    sample_prediction = game_prediction(database, session, model, home_tm, away_tm, start_time)
    data = DataManipulator(sample_prediction)
    return data


def game_prediction(database, session, regression, home_tm, away_tm, start_time, year=2019, console_out=False):
    """Predict a game versus the line, and return the information in a dictionary.

    Use console out for human readable output if desired.Cdf is a cumulative density function. SF is a survival
    function. CDF is calculated when the betting line's prediction is below the model's prediction. SF is calculated
    when the betting line's prediction is above the model's prediction.

    Args:
        database: an instantiated DBInterface class from database.dbinterface.py
        session: A SQLalchemy session object
        regression: A regression object
        start_time: Date.datetime with the date and start time of the game
        home_tm: The home team
        away_tm: The away team
        line: The betting line
        year: The year to use stats from in predicting the game
        console_out: If true, print the prediction results. Ignore otherwise
    """
    home_tm = team_name(home_tm)
    away_tm = team_name(away_tm)

    # Get Misc stats for year
    ff_list = lm.four_factors_list()
    ff_df = getters.get_pandas_df_from_table(database, session, "misc_stats_{}".format(year), ff_list)

    pred_df = prediction_df(home_tm, away_tm, ff_df)
    pred = prediction(regression, pred_df)
    # probability, function = line_probability(prediction, line, np.std(regression.residuals))

    #if console_out:
    #    prediction_result_console_output(home_tm, away_tm, line, prediction, probability)

    return {"start_time": start_time, "home_team": home_tm, "away_team": away_tm, "prediction": pred}


def prediction(reg, pred_df):
    """Generate and return a prediction for the observations in the pred_df.

    Args:
        reg: LinearRegression class from four_factors_regression.py
        pred_df: A dataframe of observations, with home and away statistics, from which to generate a prediction

    Returns:
        The predicted value generated from the regression object and the predictors"""
    return reg.results.predict(pred_df).values[0]


def console_output(home_tm, away_tm, line, prediction, probability):
    """Generate human readable printout comparing the model's predictions, the line, and the p_value of the line.

    Args:
        home_tm: The home team
        away_tm: The away team
        line: The betting line
        prediction: A prediction of the home team's margin of victory
        probability: The probability of the betting line as determined by a CDF or SF
    """
    if prediction > 0:
        print("The {} are projected to beat the {} by {} points".format(home_tm, away_tm, prediction))
        if (-1 * line) < prediction:
            print("If the model were true, the betting line's ({}) CDF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))
        else:
            print("If the model were true, the betting line's ({}) SF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))
    if prediction < 0:
        print("The {} are projected to lose to the {} by {} points".format(home_tm, away_tm, prediction))
        if (-1 * line) < prediction:
            print("If the model were true, the betting line's ({}) CDF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))
        else:
            print("If the model were true, the betting line's ({}) SF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))


def prediction_df(home_tm, away_tm, ff_df):
    """Create and return a dataframe that merges the four factors for the home and away team.

    Args:
        home_tm: The home team
        away_tm: The away team
        ff_df: Dataframe of the four factors for all teams

    Returns:
        A single row four factors data frame of the home and away team's four factors
    """
    home_ff = team_ff(home_tm, ff_df, home=True)
    away_ff = team_ff(away_tm, ff_df, home=False)
    home_ff["key"] = 1
    home_ff["const"] = 1.0  # sm.add_const does not add a constant for whatever reason
    away_ff["key"] = 1
    merged = pd.merge(home_ff, away_ff, on="key", sort=True)
    merged = merged.drop(["key"], axis=1)
    merged = merged.sort_index(axis=1)
    return merged


def team_ff(team, ff_df, home):
    """Create and return a data frame of the four factors for the specified team.

    Args:
        team: The team to extract the four factors for
        ff_df: A dataframe of the four factors
        home: Boolean which dictates if an '_h or '_a' should be appended to the team's stats

    Returns:
        The four factors, with a home or away suffix, for a team are returned as a data frame
    """
    ff_list = br_references.four_factors
    team_ff = ff_df[ff_df.team_name.str.lower() == team.lower()][ff_list]
    if home:
        team_ff = team_ff.rename(lm.append_h, axis='columns')
    else:
        team_ff = team_ff.rename(lm.append_a, axis='columns')
    return team_ff


def team_name(team):
    """Match team to a standard team name (not cap-sensitive) and return the br_references standard team name."""
    for team_name in br_references.Team:
        if team.lower() == team_name.value.lower():
            return team_name.value
