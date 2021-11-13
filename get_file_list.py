#!/usr/bin/python
""""
2021-04-08
Assessor view url prone to change for updates.
May be periods where it does not update. Need to write something to catch that.

2021-04-18
Needs logging. Currently not using this directory and should
create structure for this file and the assessor file. That way
I know what happened.
"""
import datetime
import json
import urllib.request
import os
from pathlib import Path
import pyodbc
from config import MSSQL

CURRENT_PATH = os.getcwd()
DESTINATION_PATH = os.path.join(CURRENT_PATH, "download")
PROCESSED_PATH = os.path.join(CURRENT_PATH, "processed")
ASSESSOR_DATA_PATH = os.path.join(CURRENT_PATH, "assessor_files")
LOG_PATH = os.path.join(CURRENT_PATH, "log")
DIRECTORY = [DESTINATION_PATH, PROCESSED_PATH, LOG_PATH, ASSESSOR_DATA_PATH]
CONNECTION_STRING = "DRIVER={ODBC Driver 17 for SQL Server};"+\
                    "SERVER="+MSSQL.server+\
                    ";DATABASE="+MSSQL.database+\
                    ";UID="+MSSQL.username+\
                    ";PWD="+MSSQL.password

def create_dirs(dir_list):
    """Create directories where not exist"""
    paths = dir_list
    for dirs in paths:
        if not os.path.exists(dirs):
            print("Creating", dirs, "directory.")
            os.mkdir(dirs)
        else:
            print("Directory", dirs, "already exists. Skipping.")

def conn_execute_query(connection, query, *args):
    """Connect to db and execute query.
	**Note, pyodbc cannot read messages yet. Check below for upates.
	https://github.com/mkleehammer/pyodbc/pull/765
	"""
    full_path = args
    with pyodbc.connect(connection) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query,(full_path))
            print('Query executed successfully.')
        except:
            raise RuntimeError('INFO: SQL Job did not run successfully or at all. Check query.')

def main():
    """main"""
    create_dirs(DIRECTORY)
    os.chdir(ASSESSOR_DATA_PATH)
    assessor_file_url = "https://data.lacounty.gov/api/views/"

    with urllib.request.urlopen(assessor_file_url) as url:
        data = json.loads(url.read().decode())
        assessor_file_name = "List_of_Assesor_data_file-" \
                            + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".txt"
        full_path = os.path.join(ASSESSOR_DATA_PATH, assessor_file_name)
        query = "EXEC property.proc_LoadAssessorFileData '{}';".format(full_path)
        for i in data:
            with open(assessor_file_name, "a") as files:
                if i["assetType"][:8] == "filter":
                    files.write(i["id"] + "\t" + i["name"] + "\t" + i["assetType"] + "\n")    
    conn_execute_query(CONNECTION_STRING, query)
if __name__ == "__main__":
    main()