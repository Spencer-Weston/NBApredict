"""
This module wraps the team stats, schedule, and betting line scrapers together and stores their data in the database.

If the script is called, it instantiates a Database object for database interactions and creates a SQLalchemy session
object from the Database's information. Otherwise, the scape_all() function is called with database, session, and
league year arguments specified.
"""
import os
from sqlalchemy.orm import Session

# Local Imports
from database.database import Database
from scrapers import team_scraper, season_scraper, line_scraper
import path


def scrape_all(database, session, league_year):
    """Scrape and store team stats, schedule information, and betting lines in the database

    Args:
        database: An instantiated Database object from database.database for database interactions
        session: A SQLalchemy session object
        league_year: The league year to scrape data from (i.e. 2018-2019 season is 2019)
    """
    # Insure the database folder exists
    if not os.path.isdir(path.output_directory()):
        os.mkdir(path.output_directory())

    team_scrape = team_scraper.scrape(database=database, league_year=league_year)
    season_scrape = season_scraper.scrape(database=database, session=session, league_year=league_year)
    line_scrape = line_scraper.scrape(database=database, session=session)


if __name__ == "__main__":
    db_path = path.database_file(os.path.dirname(__file__))
    db = Database(db_path)
    league_year = 2019
    session = Session(bind=db.engine)
    scrape_all(database=db, session=session, league_year=league_year)
