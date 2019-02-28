import os
from sqlalchemy.orm import sessionmaker, mapper, clear_mappers
from sqlalchemy import Table, String, Integer, Float, Column

# Local Imports
import database
from scrapers import team_scraper, season_scraper, line_scraper

# Insure the database folder exists
if not os.path.isdir("database"):
    os.mkdir("database")

# Set up database environment
db = database.Database(r"sqlite:///database//nba_db.db")
Session = sessionmaker(bind=db.engine)
session = Session()
year = 2019

team_scrape = team_scraper.scrape(database=db, year=year)
season_scrape = season_scraper.main(session=Session(), database=db, year=year)

test=2
# wordColumns = {'words': String, 'strings': String}
# db.map_table("pls_work", wordColumns)
# db.create_table()


# words_interface.map_table(wordColumns)
# word_tbl = words_interface.return_table()


# numbers = db.get_tables()["numbers"]
# words = db.get_tables()["words"]
# tbl_interface = db.get_interface("numbers")


# mapper(Template, numbers)

# t = Template()
# t.string = "two"
# t.int = 2
# t.float = 2.0001

# mapper(Template, words)


# session.add(t)
# session.commit()


# t = Table('words', db.metadata, Column('id', Integer, primary_key=True))
# #session.add(schema.Report(**report))

test= 2


