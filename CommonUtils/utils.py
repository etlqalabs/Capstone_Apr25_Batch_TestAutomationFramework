# Plesae implement execption handling here for function
import os.path

import boto3
import pandas as pd
from io import StringIO
import pandas as pd
import paramiko
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest

from Configuration.config import *

oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DATABASE}")
mysql_engine_staging = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE_STAGING}")


logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)


# data quality checks related functions

def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except  Exception as e:
        logger.error(f"File :{file_path} does not exists{e}")

def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) != 0:
            return True
        else:
            return False
    except  Exception as e:
        logger.error(f"File :{file_path} is zero byte file{e}")

def check_for_duplicate_across_the_colums(file_path,file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file passed as input parameter{file_type}")

        logger.info(f"The expected data in the file is :{df}")
        if df.duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while readuing te file {file_path}:{e}")


def check_for_duplicate_for_specific__column(file_path, file_type,column_name):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file passed as input parameter{file_type}")

        logger.info(f"The expected data in the file is :{df}")
        if df[column_name].duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while readuing te file {file_path}:{e}")


def check_for_null_values(file_path, file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file passed as input parameter{file_type}")

        logger.info(f"The expected data in the file is :{df}")
        if df.isnull().values.any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while readuing te file {file_path}:{e}")



def verify_expected_file_data_vs_actual_db_data(file_path,file_type,db_query,db_engine):
    if file_type =="csv":
        df_expected = pd.read_csv(file_path)
    elif file_type =="json":
        df_expected = pd.read_json(file_path)
    elif file_type =="xml":
        df_expected = pd.read_xml(file_path,xpath=".//item")
    else:
        raise ValueError(f"unsupported file passed as input parameter{file_type}")
    logger.info(f"The expected data in the file is :{df_expected}")
    df_actual = pd.read_sql(db_query,db_engine)
    logger.info(f"The actual data in the file is :{df_actual}")
    assert df_actual.equals(df_expected),f"expected data in {file_path} does ot match the actual data in from query {db_query}"

    def verify_expected_file_data_vs_actual_db_data(file_path, file_type, db_query, db_engine):
        if file_type == "csv":
            df_expected = pd.read_csv(file_path)
        elif file_type == "json":
            df_expected = pd.read_json(file_path)
        elif file_type == "xml":
            df_expected = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file passed as input parameter{file_type}")
        logger.info(f"The expected data in the file is :{df_expected}")
        df_actual = pd.read_sql(db_query, db_engine)
        logger.info(f"The actual data in the file is :{df_actual}")
        assert df_actual.equals(
            df_expected), f"expected data in {file_path} does ot match the actual data in from query {db_query}"

def verify_expected_db_data_vs_actual_db_data(query_expected,db_engine_expected,query_actual,db_engine_actual):
    df_expected = pd.read_sql(query_expected, db_engine_expected)
    logger.info(f"The expected data in the file is :{df_expected}")
    df_actual = pd.read_sql(query_actual,db_engine_actual)
    logger.info(f"The actual data in the file is :{df_actual}")
    assert df_actual.equals(df_expected),f"expected data from query {query_expected} does not match the actual data from query {query_actual}"


def getDataFromLinuxBox():
    try:
        logger.info("Linux  connection is being establish")
        # connect to ssh
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        # to conenct to linux server
        ssh_client.connect(hostname,username=username,password=password)
        sftp = ssh_client.open_sftp()
        # download the file from linux server to local
        sftp.get(remote_file_path,local_file_path)
        logger.info("The file from Linux is downlaoded to local")
    except Exception as e:
        logger.error(f"Error whilee connecting Linux {e}")





# Initialize a session using Boto3
s3 = boto3.client('s3')

# Read the file from S3 and return dataframe
def read_csv_from_s3(bucket_name, file_key):
    try:
        # Fetch the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)

        # Read the content of the file and load it into a Pandas DataFrame
        csv_content = response['Body'].read().decode('utf-8')  # Decode content to string
        data = StringIO(csv_content)  # Use StringIO to simulate a file-like object

        # Read the CSV data into a Pandas DataFrame
        df = pd.read_csv(data)

        # Return the DataFrame
        return df
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return None


def verify_expected_as_S3_to_actual_as_db(bucket_name,file_key,db_engine_actual,query_actual):
    # The desired path and file name in the S3 bucket
    # Call the function to read the CSV file from S3
    df_expected = read_csv_from_s3(bucket_name, file_key)
    logger.info(f"The expected data is the database is: {df_expected}")
    df_actual = pd.read_sql(query_actual, db_engine_actual)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected does not match with expected data in{query_actual}"
