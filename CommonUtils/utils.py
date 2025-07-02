# Plesae implement execption handling here for function

import pandas as pd
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