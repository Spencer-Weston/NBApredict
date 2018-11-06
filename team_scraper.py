import classification_dicts as cd
import pandas
import numpy as np
from selenium import webdriver
import time
from selenium import webdriver

eighteen_nineteen_url = r"https://www.basketball-reference.com/leagues/NBA_2019.html#all_team-stats-per_game"
webdriver_path = r"C:\Users\Spencer\Documents\installs\chromedriver_win32\chromedriver.exe"
driver = webdriver.Chrome(webdriver_path)
driver.get(eighteen_nineteen_url);
time.sleep(1) # Let the user actually see something!
search_box = driver.find_element_by_name('q')
search_box.send_keys('ChromeDriver')
search_box.submit()
time.sleep(1) # Let the user actually see something!
driver.quit()

test = cd.Position

print(test.CENTER)
