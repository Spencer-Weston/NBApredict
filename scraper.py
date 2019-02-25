from sqlalchemy.orm import sessionmaker
from scrapers import team_scraper, season_scraper, line_scraper

# Local Imports
import database

# Set up database environment
engine = database.Engine(r"sqlite:///database//nba_db.db")
Session = sessionmaker(bind=engine.engine)

