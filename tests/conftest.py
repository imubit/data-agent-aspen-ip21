import os

import numpy as np
import pandas as pd
import pytest

from data_agent_aspen_ip21.connector import AspenIp21Connector

TEST_SERVER_HOST = os.environ.get("SERVER_HOST", "localhost")
TEST_SERVER_PORT = 1433
TEST_SERVER_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
TEST_SERVER_DATABASE = "master"
TEST_SERVER_USERNAME = "sa"
TEST_SERVER_PASSWORD = "Contraseña12345678"
TEST_SERVER_DEFAULT_GROUP = "IP_AIDef"

TEST_CONN_STRING = (
    f"DRIVER={TEST_SERVER_ODBC_DRIVER};"
    f"SERVER={TEST_SERVER_HOST};"
    f"DATABASE={TEST_SERVER_DATABASE};"
    f"PORT={TEST_SERVER_PORT};"
    f"UID={TEST_SERVER_USERNAME};"
    f"PWD={TEST_SERVER_PASSWORD};"
    f"ENCRYPT=NO"
)


def _purge_db(conn):

    sql = """
DECLARE @Sql NVARCHAR(500) DECLARE @Cursor CURSOR;

SET @Cursor = CURSOR FAST_FORWARD FOR
SELECT DISTINCT sql = 'ALTER TABLE [' + tc2.TABLE_SCHEMA + '].[' +  tc2.TABLE_NAME + '] DROP [' + rc1.CONSTRAINT_NAME + '];'
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc1
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc2 ON tc2.CONSTRAINT_NAME =rc1.CONSTRAINT_NAME

OPEN @Cursor FETCH NEXT FROM @Cursor INTO @Sql

WHILE (@@FETCH_STATUS = 0)
BEGIN
Exec sp_executesql @Sql
FETCH NEXT FROM @Cursor INTO @Sql
END

CLOSE @Cursor DEALLOCATE @Cursor

EXEC sp_MSforeachtable 'DROP TABLE ?'

    """  # noqa: E501

    curs = conn.cursor()
    curs.execute(sql)
    curs.commit()


def _generate_demo_tables(conn):
    table_name = TEST_SERVER_DEFAULT_GROUP

    sql = f"""
        CREATE TABLE {table_name} (
           "NAME" varchar(255),
           "IP_DESCRIPTION" varchar(255),
           "IP_TAG_TYPE" varchar(255),
           "IP_ENG_UNITS" varchar(255),
           "IP_#_OF_TREND_VALUES" int,
           "IP_TREND_TIME" datetime,
           "IP_TREND_VALUE" real
        );

    """
    curs = conn.cursor()
    curs.execute(sql)

    df = create_random_df(["IP_TREND_VALUE"], rows=100, index_name="IP_TREND_TIME")
    df["NAME"] = "tc001.pv"
    df["IP_DESCRIPTION"] = "Temp Controller"
    df["IP_ENG_UNITS"] = "DEG"
    df["IP_#_OF_TREND_VALUES"] = 100

    for index, row in df.iterrows():
        curs.execute(
            f"INSERT INTO {table_name} "
            f"(NAME,IP_TREND_TIME,IP_TREND_VALUE,IP_DESCRIPTION,IP_ENG_UNITS) values(?,?,?,?,?)",
            row.NAME,
            index,
            row.IP_TREND_VALUE,
            row.IP_DESCRIPTION,
            row.IP_ENG_UNITS,
        )

    df = create_random_df(["IP_TREND_VALUE"], rows=100, index_name="IP_TREND_TIME")
    df["NAME"] = "fc001.pv"
    df["IP_DESCRIPTION"] = "Flow Controller"
    df["IP_ENG_UNITS"] = ""
    df["IP_#_OF_TREND_VALUES"] = 100

    for index, row in df.iterrows():
        curs.execute(
            f"INSERT INTO {table_name} "
            f"(NAME,IP_TREND_TIME,IP_TREND_VALUE,IP_DESCRIPTION,IP_ENG_UNITS) values(?,?,?,?,?)",
            row.NAME,
            index,
            row.IP_TREND_VALUE,
            row.IP_DESCRIPTION,
            row.IP_ENG_UNITS,
        )

    table_name = "IP_DIDef"

    sql = f"""
        CREATE TABLE {table_name} (
           "NAME" varchar(255),
           "IP_DESCRIPTION" varchar(255),
           "IP_TAG_TYPE" varchar(255),
           "IP_ENG_UNITS" varchar(255),
           "IP_#_OF_TREND_VALUES" int,
           "IP_TREND_TIME" datetime,
           "IP_TREND_VALUE" real
        );

    """
    curs = conn.cursor()
    curs.execute(sql)

    df = create_random_df(
        ["IP_TREND_VALUE"], rows=100, index_name="IP_TREND_TIME", val_type=np.int64
    )
    df["NAME"] = "sp001.pv"
    df["IP_DESCRIPTION"] = "Valve"
    df["IP_ENG_UNITS"] = ""
    df["IP_#_OF_TREND_VALUES"] = 100

    for index, row in df.iterrows():
        curs.execute(
            f"INSERT INTO {table_name} "
            f"(NAME,IP_TREND_TIME,IP_TREND_VALUE,IP_DESCRIPTION,IP_ENG_UNITS) values(?,?,?,?,?)",
            row.NAME,
            index,
            row.IP_TREND_VALUE,
            row.IP_DESCRIPTION,
            row.IP_ENG_UNITS,
        )

    curs.commit()


@pytest.fixture
def target_conn():
    conn = AspenIp21Connector(
        connection_string=TEST_CONN_STRING, default_group=TEST_SERVER_DEFAULT_GROUP
    )
    conn.connect()

    _purge_db(conn.odbc_conn)
    _generate_demo_tables(conn.odbc_conn)

    yield conn
    conn.disconnect()


def create_random_df(
    columns="a",
    rows=10,
    val_type=np.float64,
    initial_date="20160101",
    index=None,
    index_name="timestamp",
    freq="s",
    checkerboard_nans=False,
    order="asc",
):
    if index is None:
        index = pd.date_range(initial_date, freq=freq, periods=rows)
        if order == "desc":
            index = index[::-1]
        index.freq = None
    else:
        rows = len(index)
    if isinstance(columns, str):
        columns = list(columns)

    columns = pd.Index(data=columns)
    if val_type in [np.int64, np.int32, np.uint64, np.uint32]:
        mtrx = np.random.randint(-100, 100, (rows, len(columns))).astype(val_type)
    else:
        mtrx = np.random.randn(rows, len(columns)).astype(val_type)
    df = pd.DataFrame(mtrx, index=index, columns=columns)
    df.index.name = index_name
    if checkerboard_nans:
        coords = np.ogrid[0 : df.shape[0], 0 : df.shape[1]]
        checkerboard = (coords[0] + coords[1]) % 2 == 0
        df = df.where(checkerboard)
    return df
