import schedule
import run.all
from scrapers import getters
import time

schedule.every().day.at("09:00").do(run.all.run_all)

while True:
    schedule.run_pending()
    time.sleep(3600)
    print("Completed a check!")
