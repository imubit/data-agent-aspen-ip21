import logging
from typing import Union

import pandas as pd
import pyodbc
from data_agent.abstract_connector import (
    AbstractConnector,
    SupportedOperation,
    active_connection,
)
from pypika import (MSSQLQuery, Table, Order)

log = logging.getLogger(f"ia_plugin.{__name__}")


MAP_IP21ATTRIBUTE_2_STANDARD = {
    "NAME": "Name",
    "IP_TAG_TYPE": "Type",
    "IP_DESCRIPTION": "Description",
    "IP_ENG_UNITS": "EngUnits",
    "IP_DCS_NAME": "Path",
}

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

        self._default_group = kwargs.get('default_group')

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

        fltr = filter.split('.', 1)

        table_name = self._default_group if len(fltr) == 1 else fltr[0]
        tag_filter = filter if len(fltr) == 1 else fltr[1]

        tbl = Table(table_name)

        q = MSSQLQuery.from_(tbl).orderby(
                tbl.NAME, order=Order.asc).where(tbl.NAME.like(f'{tag_filter}%'))

        if not include_attributes:
            q = q.select(tbl.NAME)
        elif isinstance(include_attributes, list):

            for attr in include_attributes:
                q = q.select(attr)
        else:
            print('here')
            q = q.select('*')

        if max_results > 0:
            q = q.limit(max_results)

        curs = self._conn.cursor()
        curs.execute(str(q))

        columns = [column[0] for column in curs.description]
        res = {row.NAME : dict(zip(columns, row), **{'HasChildren': False}) for row in curs.fetchall()}

        return res


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

            sql = (
                "select TS,VALUE from HISTORY "
                "where NAME='%s'"
                "and PERIOD = 60*10"
                "and REQUEST = 2"
                "and REQUEST=2 and TS between TIMESTAMP'%s' and TIMESTAMP'%s'"
                % (tag, start, end)
            )

        else:

            fqn_tags = [t if '.' in t else f'{self._default_group}.{t}' for t in tags]

            # tags_by_group = {t.split('.', 1)[0]:[]  for t in fqn_tags}


            tbl = Table("IP_AIDef")

            q = (
                MSSQLQuery()
                .from_(tbl)
                .select(tbl.name, tbl.IP_TREND_TIME, tbl.IP_TREND_VALUE)
                .where(tbl.name == app_name)
            )

        curs = self._conn.cursor()
        # print(str(q))
        curs.execute(str(q))

        data = pd.read_sql(sql, self._conn)

    @active_connection
    def write_tag_values(self, tags: dict, wait_for_result: bool = True, **kwargs):
        raise RuntimeError("unsupported")
