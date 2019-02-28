from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy import inspect
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, mapper
import os

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
metadata = db.metadata


class Word(object):
    pass

class Number(object):
    pass


wordColumns = {'string': String, 'int': Integer, 'float': Float}


t = Table('words2', metadata, Column('id', Integer, primary_key=True),
    *(Column(key, value) for key, value in wordColumns.items()))
t2 = Table('numbers2', metadata, Column('id', Integer, primary_key=True),
    *(Column(key, value) for key, value in wordColumns.items()))

metadata.create_all()
mapper(Word, t)
mapper(Number, t2)

w = Word()
w.string = "stringy"
w.int = 10
w.float = 1.121

n = Number()
n.string = "ten"
n.int = 12
n.float = 21.43

session.add(w)
session.add(n)
session.commit()
test = ""


# Website Example below this line
engine = create_engine('sqlite:///:memory:', echo=True)
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    nickname = Column(String)

    def __repr__(self):
        return "<User(name='%s', fullname='%s', nickname='%s')>" % (
                            self.name, self.fullname, self.nickname)


Base.metadata.create_all(engine)

ed_user = User(name='ed', fullname='Ed Jones', nickname='edsnickname')
ed_user.name
str(ed_user.id)


Session = sessionmaker(bind=engine)
session = Session()
session.add(ed_user)
our_user = session.query(User).filter_by(name='ed').first()
our_user
ed_user.nickname = 'eddie'
session.dirty
session.commit()


for instance in session.query(User).order_by(User.id):
    print(instance.name, instance.fullname)



test = "kosher"