import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging

from CommonUtils.utils import verify_expected_file_data_vs_actual_db_data, verify_expected_db_data_vs_actual_db_data, \
    getDataFromLinuxBox, verify_expected_as_S3_to_actual_as_db, check_file_exists
from Configuration.config import *
logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

@pytest.mark.skip
@pytest.mark.smoke
@pytest.mark.regression
def test_DataQuality_supplier_data_file_availability():
    logger.info(f"Test case execution fil availability check initiated....")
    try:
       assert check_file_exists("TestData/supplier_data.json"),"File doen not exist in the location"
    except Exception as e:
        logger.error(f"Error while checking the file existance {e}")
        pytest.fail("Test case execution fil availability check  has failed")
    logger.info(f"Test case execution fil availability check  has completed....")


## Test case to check the refertial integrrity

def test_referentialIntegrity_src_staging_tgt_mysql(connect_to_mysql_database_staging,connect_to_mysql_database_target):
    query_expected = """select store_id from staging_stores order by store_id"""
    df_expected = pd.read_sql(query_expected,connect_to_mysql_database_staging)
    query_actual = """select store_id from fact_sales order by store_id"""
    df_actual= pd.read_sql(query_actual, connect_to_mysql_database_target)

    df_all_matched = df_actual[df_actual['store_id'].isin(df_expected['store_id'])]
    df_not_macthed = df_actual[~df_actual['store_id'].isin(df_expected['store_id'])]
    print(df_not_macthed)
    df_not_macthed.to_csv("LogFiles/notmatchingdata.csv",index=False)
    assert df_not_macthed.empty,"There is extra store_id in target vs source/stag"

