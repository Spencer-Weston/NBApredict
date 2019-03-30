import os
from sqlalchemy.orm import Session

# Local Imports
from database.database import Database
from scrapers import team_scraper, season_scraper, line_scraper
import path


def scrape_all(database, session, year):
    # Insure the database folder exists
    if not os.path.isdir(path.output_directory()):
        os.mkdir(path.output_directory())

    team_scrape = team_scraper.scrape(database=database, year=year)
    season_scrape = season_scraper.scrape(database=database, session=session, year=year)
    line_scrape = line_scraper.scrape(database=database, session=session)


if __name__ == "__main__":
    db_path = path.database_file(os.path.dirname(__file__))
    db = Database(db_path)
    year = 2019
    session = Session(bind=db.engine)
    scrape_all(database=db, session=session, year=year)
