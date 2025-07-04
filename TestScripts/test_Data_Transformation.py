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

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Filter_check(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for Filter has started....")
    try:

        query_expected = """select * from staging_sales where sale_date>='2024-09-10'"""
        query_actual = """select * from filtered_sales_data"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)
    except Exception as e:
        logger.error(f"test case Execution for Filter check has failed{e}")
        pytest.fail("test case Execution for Filter checkhas failed")
    logger.info(f"Test case execution for Filter check has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Router_High_check(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for Router High check has started....")
    try:


        query_expected = """select * from filtered_sales_data where region = 'High'"""
        query_actual = """select * from high_sales"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)
    except Exception as e:
        logger.error(f"test case Execution for  Router High checkhas failed{e}")
        pytest.fail("test case Execution for  Router High check has failed")
    logger.info(f"Test case execution for  Router High check has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Router_Low_check(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for Router Low check has started....")
    try:


        query_expected = """select * from filtered_sales_data where region = 'Low'"""
        query_actual = """select * from low_sales"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)
    except Exception as e:
        logger.error(f"test case Execution for  Router Low checkhas failed{e}")
        pytest.fail("test case Execution for  Router Low check has failed")
    logger.info(f"Test case execution for  Router Low check has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Aggregator_Sales_data_check(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for Aggregator_Sales_data_check has started....")
    try:



        query_expected = """select f.product_id,month(f.sale_date) as month ,year(f.sale_date) as year, sum(f.price*f.quantity) as total_sales
                            from filtered_sales_data as f group by f.product_id,month(f.sale_date),year(f.sale_date)
                            order by product_id;"""
        query_actual = """select * from monthly_sales_summary_source"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)
    except Exception as e:
        logger.error(f"test case Execution for Aggregator_Sales_data_check has failed{e}")
        pytest.fail("test case Execution for Aggregator_Sales_data_check has failed")
    logger.info(f"Test case execution for  Aggregator_Sales_data_check  has completed....")

@pytest.mark.smoke
@pytest.mark.regression
def test_DataTransformation_Aggregator_inventory_level_data_check(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for Aggregator_inventory_level_data_check has started....")
    try:

        query_expected = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id"""
        query_actual = """select * from aggregated_inventory_level"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)
    except Exception as e:
        logger.error(f"test case Execution for Aggregator_inventory_level_data_check has failed{e}")
        pytest.fail("test case Execution for Aggregator_inventory_level_data_check has failed")
    logger.info(f"Test case execution for  Aggregator_inventory_level_data_check  has completed....")

@pytest.mark.smoke
@pytest.mark.regression
@pytest.mark.post_release_check
def test_DataTransformation_Joiner_sales_products_stores_data_check(connect_to_mysql_database_staging):
    logger.info(f"Test case execution for Joiner_sales_products_stores_data_check has started....")
    try:


        query_expected = """select fs.sales_id,fs.sale_date,fs.price,fs.quantity,fs.price*fs.quantity as total_sales,
                    p.product_id,p.product_name,s.store_id,s.store_name
                     from filtered_sales_data as fs
                    inner join staging_product as p on fs.product_id = p.product_id
                    inner join staging_stores as s on s.store_id = fs.store_id"""

        query_actual = """select * from sales_with_details"""
        verify_expected_db_data_vs_actual_db_data(query_expected,connect_to_mysql_database_staging,query_actual,connect_to_mysql_database_staging)
    except Exception as e:
        logger.error(f"test case Execution for Joiner_sales_products_stores_data_check has failed{e}")
        pytest.fail("test case Execution for Joiner_sales_products_stores_data_check has failed")
    logger.info(f"Test case execution for  Joiner_sales_products_stores_data_check  has completed....")
