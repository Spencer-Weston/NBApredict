import classification_dicts as cd
import pandas
import numpy as np
import time
from selenium import webdriver

page_url = r"https://www.basketball-reference.com/leagues/NBA_2019.html"
chromedriver_path = r"C:\Users\Spencer\Documents\installs\chromedriver_win32\chromedriver.exe"
#driver = webdriver.Chrome(webdriver_path)
#driver.get(eighteen_nineteen_url)


test = cd.Position

def bball_ref_tbl_access(page_url, chromedriver_path, tbl_name):
    """Function returns access to a table(tbl_name) on the page(page_url)
    page_url - URL for basketball_reference page containing the specified table
    tbl_name - HTML name of the desired return table
    chromedriver_path - Path to the chromedriver.exe used to scrape data """

    driver = webdriver.Chrome(chromedriver_path)
    driver.get(page_url)
    table = driver.find_element_by_id(tbl_name)
    return(table)

def get_tbl_text(table):
    """Returns the text version of a HTML table"""

    return(table.text)

def find_four_factor_cols(table):
    """Returns ta"""

#table = search_box.find_element_by_id("all_misc_stats")