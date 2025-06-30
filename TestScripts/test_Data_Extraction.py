import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging

oracle_engine = create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/stag_retaildwg")

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

def test_DataExtrcation_from_sales_data_file_to_staging():
    df_expected = pd.read_csv("TestData/sales_data_Linux_remote.csv")
    query_actual = """select * from staging_sales"""
    df_actual = pd.read_sql(query_actual,mysql_engine)
    assert df_actual.equals(df_expected),"data extraction from sales file did not happen correctly"

