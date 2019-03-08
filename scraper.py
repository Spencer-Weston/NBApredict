import os

# Local Imports
import database
from scrapers import team_scraper, season_scraper, line_scraper
from sqlalchemy.orm import Session

# Insure the database folder exists
if not os.path.isdir("database"):
    os.mkdir("database")

# Set up database environment
db = database.Database(r"sqlite:///database//nba_db.db")
year = 2019
session = Session(bind=db.engine)

# team_scrape = team_scraper.scrape(database=db, year=year)
season_scrape = season_scraper.scrape(database=db, session=session, year=year)
# line_scrape = line_scraper.scrape(database=db, session=session)

session.commit()
test = 2
