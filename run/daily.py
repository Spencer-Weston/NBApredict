import schedule
import run.run
from scrapers import getters
import time

schedule.every().day().at("9:00").do(run.run)

while True:
    schedule.run_pending()
    time.sleep(3600)
    print("Completed a check!")