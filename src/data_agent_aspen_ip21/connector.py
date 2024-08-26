import logging
import sys
from functools import reduce
from operator import or_
from typing import Union

import pandas as pd
import pyodbc
from data_agent.abstract_connector import (
    AbstractConnector,
    SupportedOperation,
    active_connection,
)
from pypika import MSSQLQuery, Order, Table

log = logging.getLogger(f"da_plugin.{__name__}")

MAP_IP21ATTRIBUTE_2_STANDARD = {
    "NAME": "Name",
    "IP_TAG_TYPE": "Type",
    "IP_DESCRIPTION": "Description",
    "IP_ENG_UNITS": "EngUnits",
    "IP_DCS_NAME": "Path",
    "IP_TREND_TIME": "Timestamp",
    "IP_TREND_VALUE": "Value",
}

MAP_STANDARD_ATTR_TO_IP21 = {v: k for k, v in MAP_IP21ATTRIBUTE_2_STANDARD.items()}

MAP_TIME_FREQUENCY_TO_IP21 = {"raw data": None}


class AspenIp21Connector(AbstractConnector):
    TYPE = "aspen-ip21"
    CATEGORY = "historian"
    SUPPORTED_FILTERS = ["name", "tags_file", "time"]
    SUPPORTED_OPERATIONS = [
        SupportedOperation.READ_TAG_PERIOD,
        SupportedOperation.READ_TAG_META,
    ]
    DEFAULT_ATTRIBUTES = [
        ("Name", {"Type": "str", "Name": "Tag Name"}),
        ("EngUnits", {"Type": "str", "Name": "Eng Units"}),
        ("Type", {"Type": "str", "Name": "Type"}),
        ("Description", {"Type": "str", "Name": "Description"}),
        # ("Path", {"Type": "str", "Name": "Path"}),
    ]

    DEFAULT_ODBC_DRIVER_NAME = "AspenTech SQLplus"
    DEFAULT_SERVER_PORT = 10014
    DEFAULT_TIMEOUT = 128

    GROUP_TAG_DELIMITER = ":"

    @staticmethod
    def plugin_supported():
        return True

    @staticmethod
    def list_connection_fields():
        return {
            "server_host": {
                "name": "Server Host",
                "type": "str",
                "default_value": "",
                "optional": False,
            },
            # "server_username": {
            #     "name": "Server Username",
            #     "type": "str",
            #     "default_value": "",
            #     "optional": False,
            # },
            # "server_password": {
            #     "name": "Server Password",
            #     "type": "str",
            #     "default_value": "",
            #     "optional": False,
            # },
            "odbc_driver": {
                "name": "ODBC Driver",
                "type": "list",
                # "values": [
                #     'AspenTech ODBC driver for Production Record Manager',
                #     "ODBC Driver 18 for SQL Server"
                # ],
                "values": pyodbc.drivers(),
                "default_value": "AspenTech ODBC driver for Production Record Manager",
                "optional": False,
            },
            "connection_string": {
                "name": "Connection String",
                "type": "str",
                "default_value": "",
                "optional": True,
            },
            "default_group": {
                "name": "Default Group",
                "type": "str",
                "default_value": "",
                "optional": True,
            },
        }

    @staticmethod
    def list_registered_targets():

        if sys.platform == "win32":

            import os
            import winreg

            reg_adsa = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\AspenTech\ADSA\Caches\AspenADSA\\" + os.getlogin(),
            )
            num_sources, _, _ = winreg.QueryInfoKey(reg_adsa)

            targets = [winreg.EnumKey(reg_adsa, i) for i in range(0, num_sources)]

            return [
                {
                    "uid": f"{AspenIp21Connector.TYPE}::{srv.Name}:{srv.UniqueID}",
                    "Name": srv.Name,
                    "Host": srv.ConnectionInfo.Host,
                    "Port": srv.ConnectionInfo.Port,
                }
                for srv in targets
            ]

    @staticmethod
    def target_info(target_ref=None):
        return {"Name": "", "Endpoints": []}

    def __init__(
        self,
        conn_name="ip21_client",
        server_host="aspenone",
        server_port=None,
        server_timeout=None,
        connection_string=None,
        **kwargs,
    ):
        super(AspenIp21Connector, self).__init__(conn_name)
        self._server_host = server_host
        self._server_port = server_port or self.DEFAULT_SERVER_PORT
        self._server_timeout = server_timeout or self.DEFAULT_TIMEOUT

        self._default_group = kwargs.get("default_group")

        self._conn_string = (
            connection_string
            or f"DRIVER={self.DEFAULT_ODBC_DRIVER_NAME};"
            f"HOST={self._server_host};"
            f"PORT={self._server_port};"
            f"TIMEOUT={self._server_timeout};"
            f"MAX_ROWS=10"
        )

        self._conn = None

    @property
    def _sql_server_mode(self):
        return "sql server" in self._conn_string.lower()

    @property
    def connected(self):
        return self._conn is not None

    def connect(self):
        self._conn = pyodbc.connect(self._conn_string, autocommit=False)
        log.debug(f"Connection {self._name} established")

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

    @staticmethod
    def _standard_to_native_attr_list(attrs):
        return [
            MAP_STANDARD_ATTR_TO_IP21[a] if a in MAP_STANDARD_ATTR_TO_IP21.keys() else a
            for a in attrs
        ]

    @active_connection
    def list_tags(
        self,
        filter: Union[str, list] = "",
        include_attributes: Union[bool, list] = False,
        recursive: bool = False,
        max_results: int = 0,
    ):

        attr_list_provided = (
            isinstance(include_attributes, list) and len(include_attributes) > 0
        )

        if attr_list_provided:
            attr_to_retrieve = self._standard_to_native_attr_list(include_attributes)
            if "NAME" not in attr_to_retrieve:
                attr_to_retrieve.append("NAME")

        filters = [filter] if isinstance(filter, str) else filter

        groups_map = self._tag_list_to_group_map(filters)

        result = {}
        for grp in groups_map:

            tbl = Table(grp)

            q = (
                MSSQLQuery.from_(tbl)
                .orderby(tbl.NAME, order=Order.asc)
                .where(
                    reduce(or_, [tbl.NAME.like(f"{tag}%") for tag in groups_map[grp]])
                )
            )

            if not include_attributes:
                q = q.select(tbl.NAME)
            elif attr_list_provided:

                for attr in attr_to_retrieve:
                    q = q.select(attr)
            else:
                q = q.select("*")

            if self._sql_server_mode:
                q = q.distinct()

            self._conn.autocommit = False
            curs = self._conn.cursor()

            # if max_results > 0:
            #
            #     if self._sql_server_mode:
            #         # q = q.top(max_results)
            #         # curs.execute(str(q))
            #         # sql = f"SELECT 1; {str(q)};"
            #         sql = str(q)
            #     else:
            #         # curs.execute(f"SET MAX_ROWS {max_results};")
            #         # sql = f"SET MAX_ROWS {max_results}; {str(q)};"
            #         sql = str(q)
            #         # sql = f"DECLARE @CursorVar CURSOR; {sql};"
            #         # sql = f'exec(" SET MAX_ROWS {max_results}; {sql};  ")'
            #         # print(sql)
            #         # curs.execute('exec("@string1=?")', (sql))
            #
            #     curs.execute(sql)
            #     # curs.nextset()
            # else:
            curs.execute(str(q))

            columns = [column[0] for column in curs.description]
            rows = curs.fetchmany(max_results) if max_results > 0 else curs.fetchall()

            result.update(
                {
                    f"{grp}{self.GROUP_TAG_DELIMITER}{row.NAME}": dict(
                        zip(columns, row), **{"HasChildren": False}
                    )
                    for row in rows
                }
            )

        if not include_attributes:
            return result

        elif attr_list_provided:

            for tag in result:

                # Add standard attributes
                for a in include_attributes:
                    if a in MAP_STANDARD_ATTR_TO_IP21.keys():
                        result[tag][a] = result[tag][MAP_STANDARD_ATTR_TO_IP21[a]]

                # Remove non-asked native attributes
                result[tag] = {
                    a: result[tag][a]
                    for a in result[tag].keys()
                    if a in include_attributes or a == "HasChildren"
                }

            return result

        else:

            # Return native and standard attributes
            for tag in result:
                for a in MAP_STANDARD_ATTR_TO_IP21.keys():
                    result[tag][a] = (
                        result[tag][MAP_STANDARD_ATTR_TO_IP21[a]]
                        if MAP_STANDARD_ATTR_TO_IP21[a] in result[tag]
                        else None
                    )

        return result

    @active_connection
    def read_tag_attributes(self, tags: list, attributes: list = None):
        attr_list_provided = isinstance(attributes, list) and len(attributes) > 0

        group_map = self._tag_list_to_group_map(tags)

        res = {}
        for grp in group_map:

            tbl = Table(grp)

            q = (
                MSSQLQuery.from_(tbl)
                .orderby(tbl.NAME, order=Order.asc)
                .where(
                    reduce(or_, [tbl.NAME.like(f"{tag}%") for tag in group_map[grp]])
                )
            )

            if attr_list_provided:
                for attr in self._standard_to_native_attr_list(attributes):
                    q = q.select(attr)
                if "NAME" not in attributes:
                    q = q.select("NAME")
            else:
                q = q.select("*")
            self._conn.autocommit = False
            curs = self._conn.cursor()

            curs.execute(str(q))

            columns = [column[0] for column in curs.description]
            grp_result = {
                row.NAME: dict(zip(columns, row), **{"HasChildren": False})
                for row in curs.fetchall()
            }

            if attr_list_provided:

                for tag in grp_result:

                    # Add standard attributes
                    for a in attributes:
                        if a in MAP_STANDARD_ATTR_TO_IP21.keys():
                            grp_result[tag][a] = grp_result[tag][
                                MAP_STANDARD_ATTR_TO_IP21[a]
                            ]

                    # Remove non-asked native attributes
                    grp_result[tag] = {
                        a: grp_result[tag][a]
                        for a in grp_result[tag].keys()
                        if a in attributes or a == "HasChildren"
                    }

            else:

                # Return native and standard attributes
                for tag in grp_result:
                    for a in MAP_STANDARD_ATTR_TO_IP21.keys():
                        grp_result[tag][a] = (
                            grp_result[tag][MAP_STANDARD_ATTR_TO_IP21[a]]
                            if MAP_STANDARD_ATTR_TO_IP21[a] in grp_result[tag]
                            else None
                        )

            res.update(grp_result)

        return res

    @active_connection
    def read_tag_values(self, tags: list):
        raise RuntimeError("unsupported")

    def _tag_list_to_group_map(self, tags):
        fqn_tags = [
            (
                t
                if self.GROUP_TAG_DELIMITER in t
                else f"{self._default_group}{self.GROUP_TAG_DELIMITER}{t}"
            )
            for t in tags
        ]

        group_map = {}
        for t in fqn_tags:
            lst = t.replace("*", "%").split(self.GROUP_TAG_DELIMITER, 1)

            table_name = self._default_group if len(lst) == 1 else lst[0]
            tag = t if len(lst) == 1 else lst[1]

            group_map.setdefault(table_name, []).append(tag)

        return group_map

    @active_connection
    def read_tag_values_period(
        self,
        tags: list,
        first_timestamp=None,
        last_timestamp=None,
        time_frequency=None,
        max_results: int = 0,
        result_format="dataframe",
        progress_callback=None,
    ):

        assert result_format == "dataframe"

        if isinstance(first_timestamp, str):
            first_timestamp = pd.to_datetime(first_timestamp)
        if isinstance(last_timestamp, str):
            last_timestamp = pd.to_datetime(last_timestamp)

        first_timestamp = (
            first_timestamp.strftime("%d-%b-%y %H:%M:%S")
            if first_timestamp
            else first_timestamp
        )
        last_timestamp = (
            last_timestamp.strftime("%d-%b-%y %H:%M:%S")
            if last_timestamp
            else last_timestamp
        )
        freq = (
            MAP_TIME_FREQUENCY_TO_IP21[time_frequency.lower()]
            if time_frequency and time_frequency.lower() in MAP_TIME_FREQUENCY_TO_IP21
            else time_frequency
        )

        curs = self._conn.cursor()

        if freq:

            log.debug(f"Reading interpolated values (freq='{freq}')...")

            tbl = Table("HISTORY")

            q = MSSQLQuery().from_(tbl).select(tbl.NAME, tbl.TS, tbl.VALUE)

            if first_timestamp:
                q = q.where(tbl.TS >= first_timestamp)

            if last_timestamp:
                q = q.where(tbl.TS <= last_timestamp)

            # We ignore the groups
            tag_names = [
                (
                    t.split[self.GROUP_TAG_DELIMITER, 1][1]
                    if self.GROUP_TAG_DELIMITER in t
                    else t
                )
                for t in tags
            ]

            q = q.where(reduce(or_, [tbl.NAME.like(f"{tag}%") for tag in tag_names]))
            q = q.where(tbl.REQUEST == 2)

            # sql = (
            #     "select TS,VALUE from HISTORY "
            #     "where NAME='%s'"
            #     "and PERIOD = 60*10"
            #     "and REQUEST = 2"
            #     "and REQUEST=2 and TS between TIMESTAMP'%s' and TIMESTAMP'%s'"
            #     % (tag, start, end)
            # )

            sql = str(q)

            # IP21 does not support standard SQL TOP operator
            if max_results > 0 and not self._sql_server_mode:
                sql = f"SET MAX_ROWS {max_results}; {str(q)};"

            curs.execute(sql)

            if max_results > 0 and not self._sql_server_mode:
                curs.nextset()

            data = curs.fetchall()

        else:

            log.debug("Reading recorded values for ...")

            group_map = self._tag_list_to_group_map(tags)

            data = []

            for grp in group_map:

                if progress_callback:
                    progress_callback(f"{grp} group")

                tbl = Table(grp)

                q = (
                    MSSQLQuery()
                    .from_(tbl)
                    .select(tbl.NAME, tbl.IP_TREND_TIME, tbl.IP_TREND_VALUE)
                )

                if first_timestamp:
                    q = q.where(tbl.IP_TREND_TIME >= first_timestamp)

                if last_timestamp:
                    q = q.where(tbl.IP_TREND_TIME <= last_timestamp)

                q = q.where(
                    reduce(or_, [tbl.NAME.like(f"{tag}%") for tag in group_map[grp]])
                )

                if max_results > 0 and self._sql_server_mode:
                    q = q.top(max_results)

                sql = str(q)

                # IP21 does not support standard SQL TOP operator
                if max_results > 0 and not self._sql_server_mode:
                    sql = f"SET MAX_ROWS {max_results}; {str(q)};"

                curs.execute(sql)

                if max_results > 0 and not self._sql_server_mode:
                    curs.nextset()

                data.extend(curs.fetchall())

        df = pd.DataFrame.from_records(
            data,
            columns=[
                MAP_IP21ATTRIBUTE_2_STANDARD[column[0]] for column in curs.description
            ],
        )
        df = df.pivot(index="Timestamp", columns="Name", values="Value")
        df.index = pd.to_datetime(df.index)
        df.index.name = "timestamp"

        for tag in tags:
            short_name = (
                tag.split(self.GROUP_TAG_DELIMITER)[1]
                if self.GROUP_TAG_DELIMITER in tag
                else tag
            )

            df[short_name] = df.get(short_name, None)

        return df

    @active_connection
    def write_tag_values(self, tags: dict, wait_for_result: bool = True, **kwargs):
        raise RuntimeError("unsupported")
