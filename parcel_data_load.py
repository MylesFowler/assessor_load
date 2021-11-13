import gzip
import os
from pathlib import Path
import shutil
import sys
import pyodbc
import wget
from config import MSSQL
import get_file_list

CONNECTION_STRING = "DRIVER={ODBC Driver 17 for SQL Server};"+ \
                    "SERVER="+MSSQL.server+\
                    ";DATABASE="+MSSQL.database+\
                    ";UID="+MSSQL.username+\
                    ";PWD="+ MSSQL.password

def parse_args():
    """Parse the args"""
    param = sys.argv[1]
    return param

def gather_file_data(param_year):
    """Check for NEW Assessor data and load if available."""
    param_year = parse_args()
    with pyodbc.connect(CONNECTION_STRING) as conn:
        cursor = conn.cursor()
        query_check_tbl = "IF EXISTS (SELECT * FROM sysobjects WHERE name = 'ParcelData_{}' AND xtype = 'U') SELECT 1 else SELECT 0".format(param_year)
        query_proc_gather_data = "EXEC property.proc_AssessorFileYearCheck ?;"
        cursor.execute(query_check_tbl)
        check_result = cursor.fetchone()
        if check_result[0] ==1:
            sys.exit('Data already exists. Exiting. Check SELECT TOP 1 * FROM property.ParcelData_{}'.format(param_year))
        else:
            try:
                cursor.execute(query_proc_gather_data,(param_year))
                result = cursor.fetchone()
                data_id = result[0]
                data_filename = result[1]
                return data_id, data_filename, param_year
            except:
                   sys.exit("No new data for period {}. Exiting.".format(param_year))

def download_data(file_id):

    """Download data"""
    file_id = formatted_file_id
    filename = formatted_filename
    file_url = "https://data.lacounty.gov/api/views/{}/rows.tsv?accessType=DOWNLOAD&bom=true".format(file_id)
    print("Attempting to download {} from {}.".format(filename,file_url))
    wget.download(file_url)

def load_file(filename,year):
    """Load the new file based on existing sql procedure."""
    with pyodbc.connect(CONNECTION_STRING) as conn:
        year = file_year
        filename = formatted_filename + '.tsv'
        abs_path = os.path.join(os.getcwd(),filename)
        query = "EXEC property.proc_LoadParcelData ?, ?;"
        cursor = conn.cursor()
        print(" Executing SQL.")
        cursor.execute(query,(year, abs_path))
        zip_filename = filename + '.gz'
        print("SQL completed. Compressing and moving file to processed directory.")
        with open(abs_path, 'rb') as data_in, gzip.open(zip_filename, 'wb') as data_out:
            data_in_file = data_in
            data_out_file = data_out
            data_out_file.writelines(data_in_file)
        Path(zip_filename).rename(os.path.join(get_file_list.PROCESSED_PATH, zip_filename))
        shutil.os.remove(abs_path)

formatted_file_id, formatted_filename, file_year = gather_file_data(parse_args())

def main():
    """Main"""    
    download_data(formatted_file_id)
    load_file(formatted_filename,file_year)

if __name__ == "__main__":
    main()