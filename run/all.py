from sqlalchemy.orm import Session

# Local Imports
from database.database import Database
from predict import predict
from scrapers import scraper


def run_all():
    db = Database()
    year = 2019
    session = Session(bind=db.engine)

    scraper.scrape_all(db, session, year)
    session.commit()
    predict.predict_all(db, session, year)
    session.commit()
    session.close()

    return True


if __name__ == "__main__":
    run_all()
