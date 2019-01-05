"""
Author: Spencer Weston

Purpose: Predict takes a home team, away, team and betting line. It reports a prediction of the margin of victory
of the two teams and also reports a cumulative density function or survival function, as appropriate, for the betting
line

Args (default):
    home_tm: The home team in the matchup
    away_tm: The away team in the matchup
    line: The betting line for the matchup
    year (2019): The year to use data from
    db_url ('sqlite:///database//nba_db.db'): Path to the database holding data for predictions
"""

import numpy as np
import pandas as pd
import scipy.stats as stats
from sqlalchemy import create_engine

# Local imports
import br_references
import four_factor_regression as lm


def predict_game(reg, pred_df):
    """Generate and return a prediction for the values in the pred_df.

    Args:
        reg: LinearRegression class from four_factors_regression.py
        pred_df: A dataframe of predictive variables from which to generate a prediction

    Returns:
        The predicted value generated from the regression object and the predictors"""
    return reg.results.predict(pred_df).values[0]


def get_team_name(team):
    """Match team to a standard team name (not cap sensitive) and return the br_references standard team name"""
    for team_name in br_references.Team:
        if team.lower() == team_name.value.lower():
            return team_name.value


def create_prediction_df(home_tm, away_tm, ff_df):
    """Create and return a dataframe of the four factors for the home and away team.

    Args:
        home_tm: The home team
        away_tm: The away team
        ff_df: Dataframe of the four factors for all teams
    Returns:
        A single row four factors data frame of the home and away team's four factors
    """
    home_ff = get_team_ff(home_tm, ff_df, True)
    away_ff = get_team_ff(away_tm, ff_df, False)
    home_ff["key"] = 1
    home_ff["const"] = 1.0  # sm.add_const does not add a constant for whatever reason
    away_ff["key"] = 1
    merged = pd.merge(home_ff, away_ff, on="key", sort=True)
    merged = merged.drop(["key"], axis=1)
    merged = merged.sort_index(axis=1)
    return merged


def get_team_ff(team, ff_df, home):
    """Create and return a data frame of the four factors for the specified team

    Args:
        team: The team to extract the four factors for
        ff_df: A dataframe of the four factors
        home: Boolean which dictates if an '_h or '_a' should be appended to the team's statistics

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


def line_probability(prediction, line, std):
    """Calculate and return the CDF or SF, as appropriate, of the line if the model were true.

    "if the model were true" means that if the assumption holds that the residuals are homoscedastic and follow a
    normal distribution

    Args:
        prediction: The prediction for a game
        line: The line associated with the same game as the prediction
        std: The standard deviation of the residuals for the model used to make the prediction
    Returns:
        The likelihood of the betting line if the prediction were true
    """
    dist = stats.norm(loc=prediction, scale=std)
    line_prediction = -1 * line

    if prediction > line_prediction:
        return dist.cdf(line_prediction)
    elif prediction < line_prediction:
        return dist.sf(line_prediction)
    elif prediction == line_prediction:
        return 0.5  # If the predictions are equal, the cdf automatically equals 0.5


def main(home_tm, away_tm, line, year=2019, db_url="sqlite:///database//nba_db.db"):
    """Generates print statements that predict a game's score and present the CDF or SF or the betting line

    Cdf is a cumulative density function. SF is a survival function. CDF is calculated when the betting line's
    prediction is below the model's prediction. SF is calculated when the betting line's prediction is above the model's
    prediction.

    Args:
        home_tm: The home team
        away_tm: The away team
        line: The betting line
        year: The year to use statistics from in predicting the game
        db_url: Path to the database holding data for predictions
    """
    reg = lm.main(year=year)

    home_tm = get_team_name(home_tm)
    away_tm = get_team_name(away_tm)
    ff_list = lm.four_factors_list()

    misc_stats = "misc_stats_{}".format(year)
    engine = create_engine(db_url)
    conn = engine.connect()
    ff_df = pd.read_sql_table(misc_stats, conn)[ff_list]

    pred_df = create_prediction_df(home_tm, away_tm, ff_df)

    prediction = predict_game(reg, pred_df)

    p = line_probability(prediction, line, np.std(reg.residuals))

    if prediction > 0:
        print("The {} are projected to beat the {} by {} points".format(home_tm, away_tm, prediction))
        if (-1*line) < prediction:
            print("If the model were true, the betting line's ({}) CDF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, p))
        else:
            print("If the model were true, the betting line's ({}) SF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, p))
    if prediction < 0:
        print("The {} are projected to lose to the {} by {} points".format(home_tm, away_tm, prediction))
        if (-1*line) < prediction:
            print("If the model were true, the betting line's ({}) CDF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, p))
        else:
            print("If the model were true, the betting line's ({}) SF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, p))


if __name__ == "__main__":
    main("Portland trail Blazers", "Houston Rockets", line=1, year=2019)
