import classification_dicts as cd
import pandas
import numpy as np
import csv

def write_table_txt(table, out_file, write_type):
    """Writes the contents of table to the specified out_file"""
    file = open(out_file, write_type)
    file.write(table.text)
    print("table written to" + out_file)

def write_table_csv(table, out_file, write_type):
    pass

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
