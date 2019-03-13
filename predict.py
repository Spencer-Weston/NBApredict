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

from datetime import datetime
import numpy as np
import pandas as pd
import scipy.stats as stats
from sqlalchemy.orm import Session

# Local imports
from references import br_references
from database import DataManipulator, Database
from scrapers import helpers
from stats import four_factor_regression as lm
import general


def get_prediction(reg, pred_df):
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
    home_ff = get_team_ff(home_tm, ff_df, home=True)
    away_ff = get_team_ff(away_tm, ff_df, home=False)
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
        return dist.cdf(line_prediction), "cdf"
    elif prediction < line_prediction:
        return dist.sf(line_prediction), "sf"
    elif prediction == line_prediction:
        return 0.5  # If the predictions are equal, the cdf automatically equals 0.5


def prediction_result_console_output(home_tm, away_tm, line, prediction, probability):
    """Generate human readable printout comparing the model's predictions, the line, and the p_value of the line

    Args:
        home_tm: The home team
        away_tm: The away team
        line: The betting line
        prediction: A prediction of the home team's margin of victory
        probability: The probability of the betting line as determined by a CDF or SF
    """
    if prediction > 0:
        print("The {} are projected to beat the {} by {} points".format(home_tm, away_tm, prediction))
        if (-1*line) < prediction:
            print("If the model were true, the betting line's ({}) CDF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))
        else:
            print("If the model were true, the betting line's ({}) SF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))
    if prediction < 0:
        print("The {} are projected to lose to the {} by {} points".format(home_tm, away_tm, prediction))
        if (-1*line) < prediction:
            print("If the model were true, the betting line's ({}) CDF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))
        else:
            print("If the model were true, the betting line's ({}) SF, in relation to the prediction, would "
                  "be realized {}% of the time".format(line, probability))


def predict_game(start_time, home_tm, away_tm, line, db_conn, year=2019, console_out=False):
    """Generates print statements that predict a game's score and present the CDF or SF or the betting line

    Cdf is a cumulative density function. SF is a survival function. CDF is calculated when the betting line's
    prediction is below the model's prediction. SF is calculated when the betting line's prediction is above the model's
    prediction.

    Args:
        start_time: Date.datetime with the date and start time of the game
        home_tm: The home team
        away_tm: The away team
        line: The betting line
        db_conn: Connection to the database with the four factors and to optionally write prediction results to
        year: The year to use stats from in predicting the game
        console_out: If true, print the prediction results. Ignore otherwise
    """
    reg = lm.main(year=year)

    home_tm = get_team_name(home_tm)
    away_tm = get_team_name(away_tm)

    # Get Misc stats for year
    ff_list = lm.four_factors_list()
    misc_stats = "misc_stats_{}".format(year)
    ff_df = pd.read_sql_table(misc_stats, db_conn)[ff_list]

    pred_df = create_prediction_df(home_tm, away_tm, ff_df)

    prediction = get_prediction(reg, pred_df)

    probability, function = line_probability(prediction, line, np.std(reg.residuals))

    if console_out:
        prediction_result_console_output(home_tm, away_tm, line, prediction, probability)

    return {"start_time": start_time, "home_team": home_tm, "away_team": away_tm, "line": line,
            "prediction": prediction, "probability": probability, "function": function}


def predict_games_on_day(games, lines=False, console_out=False):
    """The function takes a date, finds the games on that date, and generates a prediction for each game

    To-Do:
        Extricate the write to database. Instead, return the prediction values (Make each function do one thing!)
        This is awful design :(
    """

    results = dict()
    if lines:
        # Some call to get betting lines for today's game
        game_lines = [4, -4]  # Sample until line_scraper is running
        for index, row in games.iterrows():
            start_time = games["start_time"][index]
            foreign_key = games["id"][index]
            predict_game(start_time=start_time, home_tm=row["home_team"], away_tm=row["away_team"],
                         line=game_lines[0], console_out=console_out)
    else:
        # Generates predictions versus a generic line of 0
        for index, row in games.iterrows():
            start_time = games["start_time"][index]
            foreign_key = games["id"][index]
            predict_game(start_time=start_time, home_tm=["home_team"], away_tm=row["away_team"], line=0,
                         console_out=console_out)
    return 2


def create_prediction_table(database, data, tbl_name):
    sql_types = data.get_sql_type()
    database.map_table(tbl_name, sql_types, "CONSTRAINTS")
    database.create_tables()
    database.insert_rows(tbl_name, data.dict_to_rows())
    database.clear_mappers()


def main(database, session, league_year, day, month, year, lines, console_out):
    """Predict games on the specified date"""

    # Get games on the specified day
    schedule = database.get_tables("sched_{}".format(league_year))
    date = datetime(year, month, day)
    games = helpers.get_games_on_day(schedule, session, date)
    games_df = pd.DataFrame(games)

    # Get lines for the games
    odds_tbl = database.get_tables("odds_{}".format(league_year))
    odds = helpers.get_spread_for_games(odds_tbl, session, games_df)

    results = predict_games_on_day(games_df, lines=lines,
                                   console_out=console_out)

    prediction_tbl = "predictions_{}".format(league_year)
    data = DataManipulator(results)
    if not database.table_exists(prediction_tbl):
        create_prediction_table()


if __name__ == "__main__":
    database = Database(r"sqlite:///database//nba_db.db")
    year = 2019
    session = Session(bind=database.engine)
    # predict_game("Sacramento Kings", "Orlando Magic", line=-5.5, year=2019)
    main(database, session, league_year=2019, day=12, month=3, year=2019, lines=True,
         console_out=True)
