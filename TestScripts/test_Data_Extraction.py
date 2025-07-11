import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging

from CommonUtils.utils import verify_expected_file_data_vs_actual_db_data, verify_expected_db_data_vs_actual_db_data, \
    getDataFromLinuxBox, verify_expected_as_S3_to_actual_as_db
from Configuration.config import *
logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)


@pytest.mark.smoke
@pytest.mark.regression
def test_DataExtraction_from_sales_data_file_to_staging(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for sales_data extarction has started....")
    #getDataFromLinuxBox()
    query_actual = """select * from staging_sales"""
    verify_expected_file_data_vs_actual_db_data("TestData/sales_data_Linux_remote.csv","csv",query_actual,connect_to_mysql_database_staging)
    logger.info(f"Test case execution for sales_data extarction has completed....")



@pytest.mark.smoke
def test_DataExtraction_from_product_data_file_to_staging(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for product_data extarction has started....")
    try:
        bucket_name = "bucket-apr-weekdays"
        file_key = "product_data/product_data.csv"
        query_actual = """select * from staging_product"""
        verify_expected_as_S3_to_actual_as_db(bucket_name,file_key,connect_to_mysql_database_staging,query_actual)
    except Exception as e:
            logger.error(f"test case Execution for stores data extrcation has failed{e}")
            pytest.fail("test case Execution for stores data extrcation has failed")
    logger.info(f"Test case execution for stores data extarction has completed....")

    

'''
def test_DataExtraction_from_supplier_data_file_to_staging():
    # Please implement the code

def test_DataExtraction_from_inventory_data_file_to_staging():
    # Please implement
    

# This is optiomised ( final test script to replicate other test scripts above)
def test_DataExtraction_from_Oracle_stores_table_file_to_staging(connect_to_oracle_database,connect_to_mysql_database_staging):
    logger.info(f"Test case execution for sales_data extarction has started....")
    try:
        query_expected = """select * from stores"""
        query_actual = """select * from staging_stores"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_oracle_database,query_actual,connect_to_mysql_database_staging)
    except Exception as e:
        logger.error(f"test case Execution for stores data extrcation has failed{e}")
        pytest.fail("test case Execution for stores data extrcation has failed")
    logger.info(f"Test case execution for stores data extarction has completed....")
'''