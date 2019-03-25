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
from sqlalchemy.orm import Session, relationship
from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint, or_
from sqlalchemy.exc import IntegrityError

# Local imports
from references import br_references
from database import DataManipulator, Database
from scrapers import getters
from stats import four_factor_regression as lm


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


def predict_game(database, session, regression, home_tm, away_tm, start_time, line, year=2019, console_out=False):
    """Generates print statements that predict a game's score and present the CDF or SF or the betting line

    Cdf is a cumulative density function. SF is a survival function. CDF is calculated when the betting line's
    prediction is below the model's prediction. SF is calculated when the betting line's prediction is above the model's
    prediction.

    Args:
        database: an instantiated Database class from database.py
        session: A SQLalchemy session object
        regression: A regression object 
        start_time: Date.datetime with the date and start time of the game
        home_tm: The home team
        away_tm: The away team
        line: The betting line
        year: The year to use stats from in predicting the game
        console_out: If true, print the prediction results. Ignore otherwise
    """
    home_tm = get_team_name(home_tm)
    away_tm = get_team_name(away_tm)

    # Get Misc stats for year
    ff_list = lm.four_factors_list()
    ff_df = getters.get_pandas_df_from_table(database, session, "misc_stats_{}".format(year), ff_list)

    pred_df = create_prediction_df(home_tm, away_tm, ff_df)

    prediction = get_prediction(regression, pred_df)

    probability, function = line_probability(prediction, line, np.std(regression.residuals))

    if console_out:
        prediction_result_console_output(home_tm, away_tm, line, prediction, probability)

    return {"start_time": start_time, "home_team": home_tm, "away_team": away_tm, "line": line,
            "prediction": prediction, "probability": probability, "function": function}


def predict_games_on_day(database, session, games, console_out=False):
    """Take a sqlalchemy query object of games, and return a prediction for each game.

    """
    results = []
    regression = lm.main(database=database, session=session, year=year)
    try:
        for game in games:
            prediction = predict_game(database=database, session=session, regression=regression, home_tm=game.home_team,
                                      away_tm=game.away_team, start_time=game.start_time, line=game.spread,
                                      console_out=console_out)
            results.append(prediction)
    except AttributeError:
        # If games doesn't contain spreads, catch the attribute error and pass a 0 line.
        # If games is missing other data, function will break.
        for game in games:
            prediction = predict_game(database=database, session=session, regression=regression, home_tm=game.home_team,
                                      away_tm=game.away_team, start_time=game.start_time, line=0,
                                      console_out=console_out)
            results.append(prediction)
    return results


def create_prediction_table(database, data, tbl_name):
    # Create columns from data
    sql_types = data.get_sql_type()
    # Add new columns
    year = tbl_name[-4:]
    schedule_name = "sched_{}".format(year)
    additional_cols = [{'game_id': [Integer, ForeignKey(schedule_name + ".id")]},
                       {"odds_id": [Integer, ForeignKey("odds_{}.id".format(year))]},
                       {"home_team_score": Integer},
                       {"away_team_score": Integer},
                       {"bet_result": String}]
    for col in additional_cols:
        sql_types.update(col)
    constraint = {UniqueConstraint: ["start_time", "home_team", "away_team"]}
    # Map prediction table
    database.map_table("predictions_{}".format(year), sql_types, constraint)

    # Get tables for relationships
    sched_tbl = database.get_table_mappings(schedule_name)
    odds_tbl = database.get_table_mappings("odds_{}".format(year))
    # Create Relationships
    if "predictions" not in sched_tbl.__mapper__.relationships.keys():
        sched_tbl.predictions = relationship(database.Template)
    if "predictions" not in odds_tbl.__mapper__.relationships.keys():
        odds_tbl.predictions = relationship(database.Template)

    database.create_tables()


def insert_prediction_table(data, session, pred_tbl, sched_tbl, odds_tbl):
    row_objects = []
    for row in data:
        row_obj = pred_tbl(**row)
        row_objects.append(row_obj)
    row_objects = get_odds_id(row_objects, session, odds_tbl)
    row_objects = update_schedule_attributes(row_objects, session, sched_tbl)

    session.add_all(row_objects)


def update_prediction_table(session, pred_tbl, sched_tbl):
    """Find and update null or 0 values in the score or bet_result columns of the prediction table."""
    score_update_objs = session.query(pred_tbl).filter(or_(pred_tbl.home_team_score == 0,
                                                           pred_tbl.away_team_score == 0)).all()
    score_update_objs = update_schedule_attributes(score_update_objs, session, sched_tbl)
    session.add_all(score_update_objs)

    bet_update_objs = session.query(pred_tbl).filter(pred_tbl.bet_result.is_(None), pred_tbl.home_team_score > 0).all()
    bet_update_objs = update_bet_results(bet_update_objs)
    session.add_all(bet_update_objs)


def update_bet_results(update_objects):
    """Take update_objects, determine the prediction result, and add the result to each row in update_objects"""
    for row in update_objects:
        score_margin = row.home_team_score - row.away_team_score
        line_inverse = row.line * -1
        prediction = row.prediction
        if score_margin == line_inverse:
            row.bet_result = "PUSH"
        elif (score_margin < line_inverse) and (prediction < line_inverse):
            row.bet_result = "WIN"
        elif (score_margin > line_inverse) and (prediction > line_inverse):
            row.bet_result = "WIN"
        else:
            row.bet_result = "LOSS"
    return update_objects



def get_game_identifiers(update_objects):
    """Return a dictionary of home_team, start_team, and start_time that forms a unique identifier

    The unique identifier works for every table in the database as home_team, away_team, and start_time are unique
    and constant for every table which involves games in the database"""
    query_dict = {"home_team": [], "away_team": [], "start_time": []}
    for row in update_objects:
        query_dict["home_team"].append(row.home_team)
        query_dict["away_team"].append(row.away_team)
        query_dict["start_time"].append(row.start_time)
    return query_dict


def get_odds_id(row_objects, session, odds_tbl):
    identifiers = get_game_identifiers(row_objects)
    odds_query = session.query(odds_tbl).filter(odds_tbl.home_team.in_(identifiers["home_team"]),
                                                odds_tbl.away_team.in_(identifiers["away_team"]),
                                                odds_tbl.start_time.in_(identifiers["start_time"])).all()
    for row in row_objects:
        for odds in odds_query:
            if row.home_team == odds.home_team and row.away_team == odds.away_team \
                    and row.start_time == odds.start_time:
                row.odds_id = odds.id
                break
    return row_objects


def update_schedule_attributes(update_objects, session, sched_tbl):
    """Match a row object with a game from schedule, add the game's information to the object, and return the objects

    Note: This matches game.id with the row_object. This is necessary for insertions, but the attribute should already
    be present on updates. Therefore, it's a superfluous action when the function is called by update functions. Use
    KWARGS to fix?

    Args:
        update_objects: Objects to update. Expects objects to contain table rows that do not have an updated score
        and/or game.id
        session: A SQLalchemy Session object
        sched_tbl: Mapped schedule table object
    """
    identifiers = get_game_identifiers(update_objects)
    sched_query = session.query(sched_tbl).filter(sched_tbl.home_team.in_(identifiers["home_team"]),
                                                  sched_tbl.away_team.in_(identifiers["away_team"]),
                                                  sched_tbl.start_time.in_(identifiers["start_time"])).all()
    for row in update_objects:
        for game in sched_query:
            if row.home_team == game.home_team and row.away_team == game.away_team \
                    and row.start_time == game.start_time:
                row.game_id = game.id
                row.home_team_score = game.home_team_score
                row.away_team_score = game.away_team_score
                break
    return update_objects


def main(database, session, league_year, day, month, year, console_out):
    """Predict games on the specified date and write the results to the database

    Args:
        database: An instantiated Database class from database.py
        session: A sqlalchemy session object for queries and writes
        league_year: The league year to work with. For example, the league year of the 2018-19 season is 2019
        day: Create a date for day, month, and year to query against
        month: Create a date for day, month, and year to query against
        year: Create a date for day, month, and year to query against
        console_out: If true, prints prediction results to the console
    """
    # Get lines for the games
    date = datetime(year, month, day)
    odds_tbl = database.get_table_mappings(["odds_{}".format(league_year)])
    games_query = getters.get_spreads_for_date(odds_tbl, session, date)
    game_spreads = [game for game in games_query]

    results = predict_games_on_day(database, session, game_spreads, console_out=console_out)

    prediction_tbl = "predictions_{}".format(league_year)
    data = DataManipulator(results)
    if not database.table_exists(prediction_tbl):
        create_prediction_table(database, data, prediction_tbl)

    sched_tbl = database.get_table_mappings("sched_{}".format(league_year))
    pred_tbl = database.get_table_mappings("predictions_{}".format(league_year))

    # Results are sent to DataManipulator in row format, so just pass data.data instead of data.dict_to_rows()
    try:
        insert_prediction_table(data.data, session, pred_tbl, sched_tbl, odds_tbl)
        session.commit()
    except IntegrityError:
        session.rollback()
        update_prediction_table(session, pred_tbl, sched_tbl)
        session.commit()
    finally:
        session.close()

    test = 2


if __name__ == "__main__":
    database = Database(r"sqlite:///database//nba_db.db")
    year = 2019
    session = Session(bind=database.engine)
    # predict_game("Sacramento Kings", "Orlando Magic", line=-5.5, year=2019)
    main(database, session, league_year=2019, day=15, month=3, year=2019, console_out=False)
