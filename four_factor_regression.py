import database as db
import classification_dicts as cd
from sqlalchemy import create_engine
from sqlalchemy import select
import pandas as pd
from sklearn import linear_model
import matplotlib.pyplot as plt
import numpy as np


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

        home_tm_ff = _get_team_ff(ff_df, home_tm, ff_list, home=True)
        home_tm_ff["key"] = 1
        home_tm_ff["mov"] = mov
        away_tm_ff = _get_team_ff(ff_df, away_tm, ff_list, home=False)
        away_tm_ff["key"] = 1

        merged = pd.merge(home_tm_ff, away_tm_ff, on="key")
        if not initialized_df:
            regression_df = merged
            initialized_df = True
        else:
            regression_df = pd.concat([regression_df, merged])
    regression_df = regression_df.drop(["key"], axis=1)

    return regression_df


def _get_team_ff(ff_df, team, ff_list, home):
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


# Variable setup
db_url = "sqlite:///database//nba_db.db"
engine = create_engine(db_url)
conn = engine.connect()

# Import and specify a list of factors to extract from database
ff_list = cd.four_factors.copy()

target_list = []
ff_list.insert(0, "team_name")
ff_list.append("wins")
ff_list.append("losses")
ff_list.append("mov")

# Database table to pandas table
misc_stats = "misc_stats_2019"
sched = "sched_2019"
ff_df = pd.read_sql_table(misc_stats, conn)[ff_list]  # FF = four factors
sched_df = pd.read_sql_table(sched, conn)


regression_df = create_ff_regression_df(ff_df, sched_df, cd.four_factors)
predictors = regression_df.loc[:, regression_df.columns != 'mov']
target = regression_df["mov"]

lm = linear_model.LinearRegression()
test = lm.fit(predictors, target)
# coefs = pd.DataFrame(zip(predictors.columns, lm.coef_), columns=["features", "estimated_coefs"])
predictions = lm.predict(predictors)
minimum = int(predictions.min()) - 1
maximum = int(predictions.max()) + 1
diag_line_x = [i for i in range(minimum, maximum)]
diag_line_y = [i for i in diag_line_x]

# Scatter plot of actual vs. predicted results
fig, ax = plt.subplots()
ax.scatter(predictions, target)
ax.set_title("Predicted vs. Actual")
ax.set_xlabel("Predicted")
ax.set_ylabel("Actual")
ax.axhline(0)
ax.plot(diag_line_x, diag_line_y)

print("FINISHED")
