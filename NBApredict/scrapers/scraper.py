"""
This module wraps the team stats, schedule, and betting line scrapers together and stores their data in the database.

If the script is called, it instantiates a DBInterface object for database interactions and creates a SQLalchemy session
object from the DBInterface's information. Otherwise, the scape_all() function is called with database, session, and
league year arguments specified.
"""
import os
from sqlalchemy.orm import Session

# Local Imports
from nbapredict.database.dbinterface import DBInterface
from nbapredict.scrapers import team_scraper, season_scraper, line_scraper
import nbapredict.configuration as configuration


def scrape_all(database, session, league_year):
    """Scrape and store team stats, schedule information, and betting lines in the database.

    Note, this only adds data to the session. Changes must be committed to be saved.

    Args:
        database: An instantiated DBInterface object from database.database for database interactions
        session: An instance of a sqlalchemy Session class bound to the database's engine
        league_year: The league year to scrape data from (i.e. 2018-2019 season is 2019)
    """
    # Insure the database folder exists
    if not os.path.isdir(configuration.output_directory()):
        os.mkdir(configuration.output_directory())

    team_scrape = team_scraper.scrape(database=database)
    season_scrape = season_scraper.scrape(database=database, session=session)
    line_scrape = line_scraper.scrape(database=database, session=session)


if __name__ == "__main__":
    db_path = configuration.database_file(os.path.dirname(__file__))
    db = DBInterface(db_path)
    league_year = 2019
    session = Session(bind=db.engine)
    scrape_all(database=db, session=session, league_year=league_year)
