import four_factor_regression as lm
import pandas as pd
import br_references
from sqlalchemy import create_engine
import statsmodels.api as sm


def predict_game(reg, pred_df):
    return reg.results.predict(pred_df).values[0]


def get_team_name(team):

    for team_name in br_references.Team:
        if team.lower() == team_name.value.lower():
            return team_name.value


def create_prediction_df(home_tm, away_tm, ff_df):
    home_ff = get_team_ff(home_tm, ff_df, True)
    away_ff = get_team_ff(away_tm, ff_df, False)
    home_ff["key"] = 1
    home_ff["const"] = 1.0
    away_ff["key"] = 1
    merged = pd.merge(home_ff, away_ff, on="key", sort=True)
    merged = merged.drop(["key"], axis=1)
    merged = merged.sort_index(axis=1)
    return merged


def get_team_ff(team, ff_df, home):
    ff_list = br_references.four_factors
    team_ff = ff_df[ff_df.team_name.str.lower() == team.lower()][ff_list]
    if home:
        team_ff = team_ff.rename(lm.append_h, axis='columns')
    else:
        team_ff = team_ff.rename(lm.append_a, axis='columns')
    return team_ff


def main(home_tm, away_tm, year=2019, db_url="sqlite:///database//nba_db.db"):
    reg = lm.main()

    home_tm = get_team_name(home_tm)
    away_tm = get_team_name(away_tm)
    ff_list = lm.four_factors_list()

    misc_stats = "misc_stats_{}".format(year)
    engine = create_engine(db_url)
    conn = engine.connect()
    ff_df = pd.read_sql_table(misc_stats, conn)[ff_list]

    pred_df = create_prediction_df(home_tm, away_tm, ff_df)

    prediction = predict_game(reg, pred_df)

    if prediction > 0:
        print("The {} are projected to beat the {} by {} points".format(home_tm, away_tm, prediction))
    if prediction < 0:
        print("The {} are projected to lose to the {} by {} points".format(home_tm, away_tm, prediction))


if __name__ == "__main__":
    main("New York Knicks", "Cleveland Cavaliers")
