import os

import pytest

from data_agent_aspen_ip21.connector import AspenIp21Connector

TEST_SERVER_HOST = os.environ.get("SERVER_HOST", "localhost")
TEST_SERVER_PORT = 1433
TEST_SERVER_ODBC_DRIVER = 'ODBC Driver 18 for SQL Server'
TEST_SERVER_DATABASE = "master"
TEST_SERVER_USERNAME = "sa"
TEST_SERVER_PASSWORD = "Contraseña12345678"

TEST_CONN_STRING = f'DRIVER={TEST_SERVER_ODBC_DRIVER};SERVER={TEST_SERVER_HOST};DATABASE={TEST_SERVER_DATABASE};PORT={TEST_SERVER_PORT};UID={TEST_SERVER_USERNAME};PWD={TEST_SERVER_PASSWORD};ENCRYPT=NO'

@pytest.fixture
def target_conn():
    conn = AspenIp21Connector(conn_string=TEST_CONN_STRING)
    conn.connect()
    yield conn
    conn.disconnect()
