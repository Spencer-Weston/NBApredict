import os
from sqlalchemy.orm import Session

# Local Imports
from database.database import Database
import path
from predict import predict
from scrapers import scraper


# Setup environment
db_path = path.database_file(os.path.dirname(__file__))
db = Database(db_path)
year = 2019
session = Session(bind=db.engine)

scraper.scrape_all(db, session, year)
session.commit()

predict.predict_all(db, session, year)
session.commit()

session.close()
