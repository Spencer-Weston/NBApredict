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

    # Get today and the last day of the season so jobs can be scheduled from today through end of season
    start_date = datetime.date(datetime.now())
    end_date = session.query(sched_tbl.start_time).order_by(sched_tbl.start_time.desc()).first()[0]
    end_date = datetime.date(end_date)

    # Get every date between now and the last day of the season
    date = start_date
    game_dates = [date]
    while date <= end_date:
        date = date + timedelta(days=1)
        game_dates.append(date)

    # Get start times for every day in date if there are games on that day
    start_times = []
    for date in game_dates:
        first_game_time = getters.get_first_game_time_on_day(sched_tbl, session, date)
        if first_game_time:
            start_times.append(first_game_time - timedelta(hours=1))

    # Transform start times into chron arguments as triggers
    cron_args = [datetime_to_dict(s_time) for s_time in start_times]
    # tests = [datetime.now() + timedelta(minutes=delta) for delta in range(2, 8, 2)]
    # cron_args = [datetime_to_dict(t_time) for t_time in tests]

    # Schedule setup
    scheduler = BackgroundScheduler()
    scheduler.add_listener(job_runs, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    # cron_args = datetime_to_dict(datetime.now() + timedelta(minutes=1))
    for kwargs in cron_args:
        scheduler.add_job(run_all, "cron", **kwargs)
    scheduler.start()
    scheduler.print_jobs()

    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)

    while len(scheduler.get_jobs()) > 0:
        time.sleep(1200)
        print("{} jobs remaining".format(len(scheduler.get_jobs())))


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
