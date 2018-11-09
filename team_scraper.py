import classification_dicts as cd
import pandas
import numpy as np
import csv
from selenium import webdriver

def bball_ref_tbl_access(page_url, chromedriver_path, tbl_name):
    """Function returns access to a table(tbl_name) on the page(page_url)
    page_url - URL for basketball_reference page containing the specified table
    tbl_name - HTML name of the desired return table
    chromedriver_path - Path to the chromedriver.exe used to scrape data """

    driver = webdriver.Chrome(chromedriver_path)
    driver.get(page_url)
    table = driver.find_element_by_id(tbl_name)
    #driver.quit() ???
    return(table)

def write_table_txt(table, out_file, write_type):
    """Writes the contents of table to the specified out_file"""
    file = open(out_file, write_type)
    file.write(table.text)
    print("table written to" + out_file)

def write_table_csv(table, out_file, write_type):

    tbl_lists = tbl_to_list_of_lists(table)

    with open(out_file, write_type, newline="") as out:
        wr = csv.writer(out, delimiter=",")
        wr.writerows(tbl_lists)
    print("csv written to" + out_file)

def tbl_to_list_of_lists(table):

    teams = [x.value for x in cd.Team]
    tbl_lines = table.text.splitlines()
    tbl_lists = []
    for line in tbl_lines:
        words = line.split()
        for i, trash in enumerate(words):
            print("current word: " + words[i])
            for team in teams:
                # Checks for matches in words surrounding word to avoid superfulous matches
                if words[i].lower() in team.lower() and (words[i-1] in team.lower() or words[i+1] in team.lower()):
                    print("in loop with:" +words[i])
                    try:
                        float(words[i])
                    except:
                        print("changing {}".word)
                        words[i] = team
                        break
        words = list(words)
        tbl_lists.append(words)
    return(tbl_lists)


# Pathes and URLs
page_url = r"https://www.basketball-reference.com/leagues/NBA_2019.html"
chromedriver_path = r"C:\Users\Spencer\Documents\installs\chromedriver_win32\chromedriver.exe"
txt_out = r"./test.txt"
csv_out = r"./test.csv"

# Get table name
tbl_name = cd.Tables.misc_stats.value

table = bball_ref_tbl_access(page_url, chromedriver_path, tbl_name)
write_table_txt(table, txt_out, "w+")
write_table_csv(table, csv_out, "w")

print("FINISHED")
