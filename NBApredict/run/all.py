"""
This module runs the entire NBA_bet project.

This module wraps the entire project into a single script with run_all() as the function which drives the script. First,
it sets up the database and session connections. Then, it scrapes all new data. Finally, it predicts all games for which
data is available. Most session.commit() calls in the project are performed here. However, note predict_all() requires
a commit during the process in order to function correctly.
"""
from sqlalchemy.orm import Session

# Local Imports
from nbapredict.database.dbinterface import DBInterface
from nbapredict.predict import predict
from nbapredict.scrapers import scraper
from nbapredict.configuration import Config


def run_all():
    """Run the entire NBA_bet project."""
    db = DBInterface()
    year = Config.get_property("league_year")
    session = Session(bind=db.engine)

    scraper.scrape_all(db, session, year)
    session.commit()

    predict.predict_all(db, session, year)
    session.commit()
    session.close()


if __name__ == "__main__":
    run_all()
