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

    db = Database()
    year = 2019
    session = Session(bind=db.engine)

    print("HERE")
    scraper.scrape_all(db, session, year)
    print("HERE2")
    session.commit()
    print("HERE3")
    predict.predict_all(db, session, year)
    session.commit()
    print("HERE4")
    session.close()

    return True


if __name__ == "__main__":
    run_all()
