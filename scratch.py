from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import inspect
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, mapper
import os

# Local Imports
import database
from scrapers import team_scraper, season_scraper, line_scraper

# Set up database environment
db = database.Database(r"sqlite:///database//nba_db.db")
year = 2019
session = Session(bind=db.engine)

odds_table = db.get_table_mappings(["odds_2019"])

row_objects = []
rows = session.query(odds_table).all()
for row in rows:
    row.home_team = row.home_team.upper()
    row.away_team = row.away_team.upper()
    row_objects.append(row)
session.add_all(row_objects)
session.commit()

