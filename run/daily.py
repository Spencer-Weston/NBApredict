from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from datetime import datetime, timedelta
import logging
from sqlalchemy.orm import Session
import time


# Local Imports
from database import getters
from database.database import Database
from run.all import run_all


def run_all_daily():
    # Database setup
    database = Database()
    year = 2019
    session = Session(bind=database.engine)
    sched_tbl = database.get_table_mappings("sched_{}".format(year))

    # Schedule setup
    scheduler = BackgroundScheduler()
    scheduler.add_listener(job_runs, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    cron_args = datetime_to_dict(datetime.now() + timedelta(minutes=1))
    job = scheduler.add_job(run_all, "cron", **cron_args)
    test = scheduler.start()
    scheduler.print_jobs()

    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)

    while True:
        print("in daily infinite while loop")
        date = datetime.date(datetime.now())
        first_game_time = getters.get_first_game_time_on_day(sched_tbl, session, date)
        cron_args = datetime_to_dict(datetime.now() - timedelta(hours=1))
        job = scheduler.add_job(run_all, "cron", **cron_args)
        test = 2
    # date = datetime.date(datetime.now())
    # first_game_time = getters.get_first_game_time_on_day(sched_tbl, session, date)
    # next_run_time = first_game_time - timedelta(hours=1)
    # cron_args = datetime_to_dict(next_run_time)
    #
    # scheduler.reschedule_job(job.id, "cron", **cron_args)
    #
    # print("not time to run yet @ {}!".format(datetime.now()))
    # time.sleep(30)
    # print("Completed a check @ {}!".format(datetime.now()))


def datetime_to_dict(d_time):
    time_dict = {"year": d_time.year, "month": d_time.month, "day": d_time.day, "hour": d_time.hour,
                 "minute": d_time.minute}
    return time_dict


def job_runs(event):
    if event.exception:
        print('The job did not run')
    else:
        print('The job completed @ {}'.format(datetime.now()))


if __name__ == "__main__":
    run_all_daily()
