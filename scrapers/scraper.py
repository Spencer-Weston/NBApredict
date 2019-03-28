import os
from sqlalchemy.orm import Session

# Local Imports
from database import database
from scrapers import team_scraper, season_scraper, line_scraper
import path

# Insure the database folder exists
if not os.path.isdir(path.output_directory()):
    os.mkdir(path.output_directory())


# Set up database environment
db_path = path.database_file(os.path.dirname(__file__))
db = database.Database(db_path)
year = 2019
session = Session(bind=db.engine)

team_scrape = team_scraper.scrape(database=db, year=year)
season_scrape = season_scraper.scrape(database=db, session=session, year=year)
line_scrape = line_scraper.scrape(database=db, session=session)

session.commit()
