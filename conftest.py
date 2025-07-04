import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest

from Configuration.config import *

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

@pytest.fixture()
def connect_to_oracle_database():
    logger.info("Oracle conenction is getting established")
    oracle_engine = create_engine(
        f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DATABASE}").connect()
    logger.info("Oracle conenction has been established")
    yield oracle_engine
    oracle_engine.close()
    logger.info("Oracle conenction has been closed")

@pytest.fixture()
def connect_to_mysql_database_staging():
    logger.info("mysql conenction is getting established")
    mysql_engine_staging = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE_STAGING}").connect()
    logger.info("mysql conenction has been established")
    yield mysql_engine_staging
    mysql_engine_staging.close()
    logger.info("mysql conenction has been closed")

@pytest.fixture()
def connect_to_mysql_database_target():
    logger.info("mysql conenction is getting established")
    mysql_engine_target = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE_TARGET}").connect()
    logger.info("mysql conenction has been established")
    yield mysql_engine_target
    mysql_engine_target.close()
    logger.info("mysql conenction has been closed")
