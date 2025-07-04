import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import logging

from CommonUtils.utils import verify_expected_file_data_vs_actual_db_data, verify_expected_db_data_vs_actual_db_data
from Configuration.config import *

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

@pytest.mark.skip
def test_DataLoad_for_Monthly_sales_summary_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):
    logger.info(f"Test case execution for Monthly_sales_summary_check check has started....")
    try:
        query_expected = """select * from monthly_sales_summary_source"""
        query_actual = """select * from monthly_sales_summary"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)
    except Exception as e:
        logger.error(f"test case Execution for Monthly_sales_summary_check has failed{e}")
        pytest.fail("test case Execution for Monthly_sales_summary_check has failed")
    logger.info(f"Test case execution for Monthly_sales_summary_check has completed....")

@pytest.mark.skip
def test_DataLoad_for_fact_sales_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):
    logger.info(f"Test case execution for fact_sales_check check has started....")
    try:
        query_expected = """select * from sales_with_details"""
        query_actual = """select * from fact_sales"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)
    except Exception as e:
        logger.error(f"test case Execution for fact_sales_check has failed{e}")
        pytest.fail("test case Execution for fact_sales_check has failed")
    logger.info(f"Test case execution for fact_sales_check has completed....")

@pytest.mark.skip
def test_DataLoad_for_fact_inventory_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):
    logger.info(f"Test case execution for fact_inventory_check check has started....")
    try:
        query_expected = """select * from staging_inventory"""
        query_actual = """select * from fact_inventory"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)
    except Exception as e:
        logger.error(f"test case Execution for fact_inventory_check has failed{e}")
        pytest.fail("test case Execution for fact_inventory_check has failed")
    logger.info(f"Test case execution for fact_inventory_check has completed....")

def test_DataLoad_for_inventory_level_stores_check(connect_to_mysql_database_staging,connect_to_mysql_database_target):
    logger.info(f"Test case execution for inventory_level_stores_check check has started....")
    try:
        query_expected = """select store_id,total_inventory from aggregated_inventory_level"""
        query_actual = """select store_id,cast(total_inventory as Double) as total_inventory from inventory_levels_by_store"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_target)
    except Exception as e:
        logger.error(f"test case Execution for inventory_level_stores_check has failed{e}")
        pytest.fail("test case Execution for inventory_level_stores_check has failed")
    logger.info(f"Test case execution for inventory_level_stores_check has completed....")
