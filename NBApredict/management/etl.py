""" ETL (Extract Transform Load) manages data scraping, modification, table creation, and data loading.

Main() calls the necessary ETL functions from scrapers and management.tables for all tables.

Tables:
    teams
    schedule
    odds
    team_stats

"""

from datetime import datetime
from datatotable.database import Database
from datatotable.data import DataOperator
from nbapredict.configuration import Config
import nbapredict.management
import nbapredict.management.conversion as convert
from nbapredict.management.tables import teams, team_stats, odds, schedule
from nbapredict.scrapers import team_scraper, line_scraper, season_scraper


def main(db):
    year = Config.get_property("league_year")
    session = nbapredict.management.Session(bind=db.engine)

    # ~~~~~~~~~~~~~
    # Teams
    # ~~~~~~~~~~~~~
    team_dict = team_scraper.scrape()
    teams_data = DataOperator({"team_name": team_dict["team_name"]})
    teams_tbl_name = "teams_{}".format(year)
    if not db.table_exists(teams_tbl_name):
        teams.create_team_table(db=db, teams_data=teams_data, tbl_name=teams_tbl_name)
        teams_tbl = db.table_mappings[teams_tbl_name]
        session.add_all([teams_tbl(**row) for row in teams_data.rows])
        session.commit()
        del teams_tbl

    # ~~~~~~~~~~~~~
    # Team Stats
    # ~~~~~~~~~~~~~
    team_stats_tbl_name = "team_stats_{}".format(year)
    teams_tbl = db.table_mappings[teams_tbl_name]
    team_dict['team_id'] = team_dict.pop('team_name')
    team_dict['team_id'] = convert.values_to_foreign_key(session=session, foreign_tbl=teams_tbl, foreign_key="id",
                                                         foreign_value="team_name", child_data=team_dict['team_id'])
    # When team_stats_tbl is created, the teams_tbl automap object is changed. The changed format does not follow
    # the expected behavior of an automapped table. I suspect this is because a relationship is established.
    # If we reloaded, teams_tbl works fine. Therefore, delete the variable here for now
    del teams_tbl
    team_dict['scrape_date'] = [datetime.date(s_time) for s_time in team_dict['scrape_time']]
    team_stats_data = DataOperator(team_dict)
    if not db.table_exists(team_stats_tbl_name):
        team_stats.create_table(db=db, team_stats_data=team_stats_data, tbl_name=team_stats_tbl_name)
        team_stats_tbl = db.table_mappings[team_stats_tbl_name]
        session.add_all([team_stats_tbl(**row) for row in team_stats_data.rows])
        session.commit()
    else:
        team_stats_tbl = db.table_mappings[team_stats_tbl_name]
        team_stats.insert(session, team_stats_tbl, team_stats_data)

    # ~~~~~~~~~~~~~
    # Schedule
    # ~~~~~~~~~~~~~
    schedule_dict = season_scraper.scrape()
    schedule_data = DataOperator(schedule_dict)
    teams_tbl = db.table_mappings['teams_{}'.format(year)]
    schedule_data = schedule.format_data(session=session, schedule_data=schedule_data,
                                         team_tbl=teams_tbl, team_stats_tbl=team_stats_tbl)
    schedule_tbl_name = "schedule_{}".format(year)
    if not db.table_exists(schedule_tbl_name):
        schedule.create_table(db, schedule_data, schedule_tbl_name, teams_tbl, team_stats_tbl)
        schedule_tbl = db.table_mappings[schedule_tbl_name]
        session.add_all([schedule_tbl(**row) for row in schedule_data.rows])
        session.commit()
    else:
        schedule_tbl = db.table_mappings[schedule_tbl_name]
        update_rows = schedule.update_table(session, schedule_data, schedule_tbl, team_stats_tbl)
        session.add_all(update_rows)
        session.commit()

    # ~~~~~~~~~~~~~
    # Odds
    # ~~~~~~~~~~~~~
    odds_dict = line_scraper.scrape()
    odds_data = None
    if odds_dict:
        odds_dict = odds.format_data(session, odds_dict, teams_tbl, schedule_tbl)
        odds_data = DataOperator(odds_dict)
    # Evaluate if you have the correct columns in odds_data (i.e. home\away team id's)
    odds_tbl_name = "odds_{}".format(year)
    if not db.table_exists(odds_tbl_name) and odds_data:
        odds.create_table(db, odds_tbl_name, odds_data, schedule_tbl)
        odds_tbl = db.table_mappings[odds_tbl_name]
        session.add_all(odds_tbl(**row) for row in odds_data.rows)
        session.commit()
    elif odds_data:
        odds_tbl = db.table_mappings[odds_tbl_name]
        session.add_all(odds_tbl(**row) for row in odds_data.rows)
        session.commit()
        odds.update_table(session, odds_tbl, odds_data)
        session.commit()
        odds.delete(session, odds_tbl)

    t = 2


if __name__ == "__main__":
    db = Database("test", Config.get_property("outputs"))
    main(db)
    t = 2
