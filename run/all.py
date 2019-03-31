from datetime import datetime
import os
from sqlalchemy.orm import Session

# Local Imports
from database.database import Database
import path
from predict import predict
from scrapers import scraper


def run_all():
    if __name__ != "__main__":
        file_path = os.getcwd()
        db_path = path.database_file(file_path)
    # Setup environment
    else:
        db_path = path.database_file(os.path.dirname(__file__))
    db = Database(db_path)
    year = 2019
    session = Session(bind=db.engine)

    scraper.scrape_all(db, session, year)
    session.commit()

    predict.predict_all(db, session, year)
    session.commit()

    session.close()

    now = datetime.now()
    print("run_all() completed at {}".format(now))


if __name__ == "__main__":
    run_all()
