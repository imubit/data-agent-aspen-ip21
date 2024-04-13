import logging
from typing import Union

import pandas as pd
import pyodbc
from data_agent.abstract_connector import (
    AbstractConnector,
    SupportedOperation,
    active_connection,
)
from pypika import MSSQLQuery, Table

log = logging.getLogger(f"ia_plugin.{__name__}")


class AspenIp21Connector(AbstractConnector):
    TYPE = "aspen-ip21"
    CATEGORY = "historian"
    SUPPORTED_FILTERS = ["name", "tags_file", "time"]
    SUPPORTED_OPERATIONS = [
        SupportedOperation.READ_TAG_PERIOD,
        SupportedOperation.READ_TAG_META,
    ]
    DEFAULT_ATTRIBUTES = [
        ("tag", {"Type": "str", "Name": "Tag Name"}),
        ("engunits", {"Type": "str", "Name": "Units"}),
        ("typicalvalue", {"Type": "str", "Name": "Typical Value"}),
        ("descriptor", {"Type": "str", "Name": "Description"}),
        ("pointsource", {"Type": "str", "Name": "Point Source"}),
        # ('pointtype', {
        #     'Type': str,
        #     'Name': 'Type'
        # }),
        ("compressing", {"Type": "int", "Name": "Compression"}),
        ("changer", {"Type": "str", "Name": "Modified By"}),
    ]

    DEFAULT_ODBC_DRIVER_NAME = "AspenTech SQLplus"
    DEFAULT_SERVER_PORT = 10014
    DEFAULT_TIMEOUT = 128

    @staticmethod
    def list_connection_fields():
        return {
            "server_name": {
                "name": "Server Name",
                "type": "list",
                "values": [
                    i["Name"] for i in AspenIp21Connector.list_registered_targets()
                ],
                "default_value": "",
                "optional": False,
            },

            "default_group": {
                "name": "Default Group",
                "type": "str",
                "default_value": "",
                "optional": True,
            }
        }

    @staticmethod
    def list_registered_targets():
        # sources = tagreader.list_sources("aspenone")
        ret = []

        # for srv in sources:
        #     ret.append(
        #         {
        #             "uid": f"{AspenIp21Connector.TYPE}::{srv.Name}:{srv.UniqueID}",
        #             "Name": srv.Name,
        #             "Host": srv.ConnectionInfo.Host,
        #             "Port": srv.ConnectionInfo.Port,
        #         }
        #     )

        return ret

    @staticmethod
    def target_info(host=None):
        return {"Name": "absolute-fake", "Endpoints": []}

    def __init__(
        self,
        conn_name="ip21_client",
        server_host="aspenone",
        server_port=None,
        server_timeout=None,
        conn_string=None,
        **kwargs,
    ):
        super(AspenIp21Connector, self).__init__(conn_name)
        self._server_host = server_host
        self._server_port = server_port or self.DEFAULT_SERVER_PORT
        self._server_timeout = server_timeout or self.DEFAULT_TIMEOUT

        self._conn_string = (
            conn_string
            or f"DRIVER={self.DEFAULT_ODBC_DRIVER_NAME};HOST={self._server_host};PORT={self._server_port};TIMEOUT={self._server_timeout}"
        )

        self._conn = None

    @property
    def connected(self):
        return self._conn is not None

    def connect(self):
        self._conn = pyodbc.connect(self._conn_string)

    @property
    def odbc_conn(self):
        return self._conn

    @active_connection
    def disconnect(self):
        self._conn.close()
        self._conn = None

    @active_connection
    def connection_info(self):
        return {
            "OneLiner": f"[{self.TYPE}] 'ODBC://",
            "ServerName": self._server_host,
            "Description": "",
            "Version": "",
            # "Host": self._server_host,
            # "Port": self._server_port,
        }

    @active_connection
    def list_tags(
        self,
        filter: Union[str, list] = "",
        include_attributes: Union[bool, list] = False,
        recursive: bool = False,
        max_results: int = 0,
    ):

        grp, name = filter.split('.', 1)

        tbl = Table("pg_stat_activity")

    @active_connection
    def read_tag_attributes(self, tags: list, attributes: list = None):
        pass

    @active_connection
    def read_tag_values(self, tags: list):
        raise RuntimeError("unsupported")

    @active_connection
    def read_tag_values_period(
        self,
        tags: list,
        first_timestamp=None,
        last_timestamp=None,
        time_frequency=None,
        max_results=None,
        result_format="dataframe",
        progress_callback=None,
    ):
        if time_frequency:
            tbl = Table("HISTORY")

            q = (
                MSSQLQuery()
                .from_(tbl)
                .select(tbl.name, tbl)
                .where(tbl.application_name == app_name)
            )

            sql = sql = (
                "select TS,VALUE from HISTORY "
                "where NAME='%s'"
                "and PERIOD = 60*10"
                "and REQUEST = 2"
                "and REQUEST=2 and TS between TIMESTAMP'%s' and TIMESTAMP'%s'"
                % (tag, start, end)
            )

        else:
            tbl = Table("IP_AIDef")

            q = (
                MSSQLQuery()
                .from_(tbl)
                .select(tbl.name, tbl.IP_TREND_TIME, tbl.IP_TREND_VALUE)
                .where(tbl.name == app_name)
            )

        with self.sql_execute(str(q)) as curs:
            curs.fetchall()

        data = pd.read_sql(sql, self._conn)

    @active_connection
    def write_tag_values(self, tags: dict, wait_for_result: bool = True, **kwargs):
        raise RuntimeError("unsupported")
