from lxml import html
import datetime
import pytz

from helpers.br_references import Team

TEAM_NAME_TO_TEAM = {
    member.value: member
    for (_, member) in Team.__members__.items()
}

TEAM_NAME_TO_TEAM["NEW ORLEANS/OKLAHOMA CITY HORNETS"] = Team.NEW_ORLEANS_OKLAHOMA_CITY_HORNETS


def parse_start_time(formatted_date, formatted_time_of_day):
    if formatted_time_of_day is not None and formatted_time_of_day not in ["", " "]:
        # Starting in 2018, the start times had a "p" or "a" appended to the end
        # Between 2001 and 2017, the start times had a "pm" or "am"
        #
        # https://www.basketball-reference.com/leagues/NBA_2018_games.html
        # vs.
        # https://www.basketball-reference.com/leagues/NBA_2001_games.html
        is_prior_format = formatted_time_of_day[-2:] == "am" or formatted_time_of_day[-2:] == "pm"

        # If format contains only "p" or "a" add an "m" so it can be parsed by datetime module
        if is_prior_format:
            combined_formatted_time = formatted_date + " " + formatted_time_of_day
        else:
            combined_formatted_time = formatted_date + " " + formatted_time_of_day + "m"

        if is_prior_format:
            start_time = datetime.datetime.strptime(combined_formatted_time, "%a, %b %d, %Y %I:%M %p")
        else:
            start_time = datetime.datetime.strptime(combined_formatted_time, "%a, %b %d, %Y %I:%M%p")
    else:
        start_time = datetime.datetime.strptime(formatted_date, "%a, %b %d, %Y")

    # All basketball reference times seem to be in Eastern
    est = pytz.timezone("US/Eastern")
    localized_start_time = est.localize(start_time)

    # When localized_start_time calls and returns astimezone(pytz.utc), the values are converted to UTC.
    # In this call, the day of the game can be changed. For example, an 10pm game on October 16th may be converted to a
    # 2am game in on October 17th in UTC. To avoid this effect, return localized_start_time
    return localized_start_time
    # return localized_start_time.astimezone(pytz.utc)


def current_time():
    now = datetime.datetime.now()
    est = pytz.timezone("US/Eastern")
    localized_now_time = est.localize(now)
    return localized_now_time.astimezone(pytz.utc)


def parse_game(row):
    start_time = parse_start_time(formatted_date=row[0].text_content(), formatted_time_of_day=row[1].text_content())

    # Test existed to check for games that haven't been played. Replaced to default unplayed games to 0-0 score
    #try:
    #    test = int(row[3].text_content())
    #except:
    #    print("invalid test")

    try:
        away_team_score = int(row[3].text_content())
        home_team_score = int(row[5].text_content())
    except:
        away_team_score = 0
        home_team_score = 0
    return {
        "start_time": start_time,
        "away_team": TEAM_NAME_TO_TEAM[row[2].text_content().upper()],
        "away_team_score": away_team_score,
        "home_team": TEAM_NAME_TO_TEAM[row[4].text_content().upper()],
        "home_team_score": home_team_score,
    }


def parse_schedule(page):
    tree = html.fromstring(page)
    rows = tree.xpath('//table[@id="schedule"]//tbody/tr')
    schedule = []
    for row in rows:
        if row.text_content() != "Playoffs":
            start_time = parse_start_time(formatted_date=row[0].text_content(), formatted_time_of_day=row[1].text_content())
            # now = current_time()
            # Scrape all data up to 'yesterday'; Don't scrape for today as in progress games create errors
            # if (start_time.month == now.month) and (start_time.day > (now.day - 1)):
            #    break
            # elif start_time > now:
            #    break
            schedule.append(parse_game(row))
        if row.text_content() == "Playoffs":
            print('here')
    return schedule


def parse_schedule_for_month_url_paths(page):
    tree = html.fromstring(page)
    months = tree.xpath('//div[@id="content"]/div[@class="filter"]/div[not(contains(@class, "current"))]/a')
    return list(map(lambda month: month.attrib['href'], months))
