from datatotable.database import Database
from datatotable.data import DataOperator
from nbapredict.scrapers import team_scraper, line_scraper, season_scraper
from nbapredict.configuration import Config


def create_team_table(db, teams):
    columns = teams.columns
    db.map_table(tbl_name="teams_{}".format(Config.get_property("league_year")), columns=columns)
    db.create_tables()
    db.clear_mappers()


def create_team_stats_table(db, team_stats):
    pass


def main(db, session):
    year = Config.get_property("league_year")
    team_dict = team_scraper.scrape(database=db)  # ToDo: Reformat team_scraper to not need DB
    if not db.table_exists("teams_{}".format(year)):
        teams = DataOperator({"team_name": team_dict["team_name"]})
        create_team_table(db, teams)

    if not db.table_exists("team_stats_{}".format(year)):
        pass



if __name__ == "__main__":
    from sqlalchemy.orm import Session
    db = Database("test", Config.get_property("database"))
    session = Session(db.engine)
    main(db, session)
    t=2