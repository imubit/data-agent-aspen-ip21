import datetime
from conftest import TEST_CONN_STRING

from data_agent_aspen_ip21.connector import AspenIp21Connector


# Demo tags

# IP_AnalogDef.AvailableMemoryVals (has history)
# testtag


def test_sanity():
    conn = AspenIp21Connector(conn_string=TEST_CONN_STRING)
    assert not conn.connected
    conn.connect()
    assert conn.connected

    assert conn.TYPE == "aspen-ip21"

    info = conn.connection_info()
    # assert info["ServerName"] == TEST_SERVER_NAME
    # assert info["Version"] == TEST_SERVER_VERSION
    assert info["Description"] == ""

    conn.disconnect()
    assert not conn.connected


def test_list_tags(target_conn):
    tags = target_conn.list_tags()

    assert tags == {'fc001.pv': {'NAME': 'fc001.pv', 'HasChildren': False},
                    'tc001.pv': {'NAME': 'tc001.pv', 'HasChildren': False}}

    tags = target_conn.list_tags(include_attributes=['IP_DESCRIPTION', 'Description', 'EngUnits'])

    assert tags == {'fc001.pv': {'IP_DESCRIPTION': 'Flow Controller', 'Description': 'Flow Controller', 'EngUnits': ''},
                    'tc001.pv': {'IP_DESCRIPTION': 'Temp Controller', 'Description': 'Temp Controller',
                                 'EngUnits': 'DEG'}}
