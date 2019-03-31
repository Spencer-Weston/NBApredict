import schedule
from run.all import run_all
import time
from datetime import datetime


def run_all_daily():
    # schedule.every().day.at("09:00").do(run_all)
    schedule.every(2).minutes.do(run_all)

    while True:
        schedule.run_pending()
        time.sleep(30)
        print("Completed a check @ {}!".format(datetime.now()))
