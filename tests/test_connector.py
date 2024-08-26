import pandas as pd
from conftest import TEST_CONN_STRING

from data_agent_aspen_ip21.connector import AspenIp21Connector


def test_sanity():
    conn = AspenIp21Connector(connection_string=TEST_CONN_STRING)
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
    assert tags == {
        "IP_AIDef:fc001.pv": {"NAME": "fc001.pv", "HasChildren": False},
        "IP_AIDef:tc001.pv": {"NAME": "tc001.pv", "HasChildren": False},
    }

    tags = target_conn.list_tags(filter=["fc001.pv", "tc001.pv"])
    assert tags == {
        "IP_AIDef:fc001.pv": {"NAME": "fc001.pv", "HasChildren": False},
        "IP_AIDef:tc001.pv": {"NAME": "tc001.pv", "HasChildren": False},
    }

    tags = target_conn.list_tags(filter=["IP_AIDef:fc001.pv", "tc001*"])
    assert tags == {
        "IP_AIDef:fc001.pv": {"NAME": "fc001.pv", "HasChildren": False},
        "IP_AIDef:tc001.pv": {"NAME": "tc001.pv", "HasChildren": False},
    }

    tags = target_conn.list_tags(
        filter=["IP_AIDef:fc001.pv", "IP_DIDef:*"],
        include_attributes=["Name", "IP_DESCRIPTION", "Description", "EngUnits"],
    )
    assert tags == {
        "IP_AIDef:fc001.pv": {
            "Name": "fc001.pv",
            "IP_DESCRIPTION": "Flow Controller",
            "HasChildren": False,
            "Description": "Flow Controller",
            "EngUnits": "",
        },
        "IP_DIDef:sp001.pv": {
            "Name": "sp001.pv",
            "IP_DESCRIPTION": "Valve",
            "HasChildren": False,
            "Description": "Valve",
            "EngUnits": "",
        },
    }

    tags = target_conn.list_tags(
        include_attributes=["IP_DESCRIPTION", "Description", "EngUnits"]
    )
    assert tags == {
        "IP_AIDef:fc001.pv": {
            "IP_DESCRIPTION": "Flow Controller",
            "HasChildren": False,
            "Description": "Flow Controller",
            "EngUnits": "",
        },
        "IP_AIDef:tc001.pv": {
            "IP_DESCRIPTION": "Temp Controller",
            "HasChildren": False,
            "Description": "Temp Controller",
            "EngUnits": "DEG",
        },
    }

    tags = target_conn.list_tags(
        max_results=3, include_attributes=["IP_DESCRIPTION", "Description", "EngUnits"]
    )
    assert tags == {
        "IP_AIDef:fc001.pv": {
            "IP_DESCRIPTION": "Flow Controller",
            "HasChildren": False,
            "Description": "Flow Controller",
            "EngUnits": "",
        },
        "IP_AIDef:tc001.pv": {
            "IP_DESCRIPTION": "Temp Controller",
            "HasChildren": False,
            "Description": "Temp Controller",
            "EngUnits": "DEG",
        },
    }


def test_read_tag_values_period(target_conn):

    df = target_conn.read_tag_values_period(["tc001.pv"])
    assert len(df) == 100
    assert list(df.columns) == ["tc001.pv"]

    def progress_callback(msg):
        print(msg)

    df = target_conn.read_tag_values_period(
        ["fc001.pv", "tc001.pv"], progress_callback=progress_callback
    )

    assert len(df) == 100
    assert list(df.columns) == ["fc001.pv", "tc001.pv"]

    df = target_conn.read_tag_values_period(["fc001.pv"], max_results=10)
    assert len(df) == 10
    assert list(df.columns) == ["fc001.pv"]

    df = target_conn.read_tag_values_period(
        ["fc001.pv"], max_results=10, first_timestamp="20160101 00:01"
    )
    assert df.index[0] == pd.Timestamp("20160101 00:01")

    df = target_conn.read_tag_values_period(
        ["fc001.pv"], last_timestamp="20160101 00:01"
    )
    assert df.index[-1] == pd.Timestamp("20160101 00:01")

    df = target_conn.read_tag_values_period(
        ["IP_AIDef:fc001.pv", "IP_DIDef:sp001.pv"], last_timestamp="20160101 00:01"
    )
    assert df.index[-1] == pd.Timestamp("20160101 00:01")


def test_read_tag_attributes(target_conn):
    # Test PI attribute
    res = target_conn.read_tag_attributes(["fc001.pv", "tc001.pv"])

    assert res["fc001.pv"]["Name"] == "fc001.pv"
    assert len(res["fc001.pv"]) == 15
    assert res["tc001.pv"]["Name"] == "tc001.pv"
    assert len(res["tc001.pv"]) == 15

    res = target_conn.read_tag_attributes(["IP_AIDef:fc001.pv", "IP_AIDef:tc001.pv"])

    assert res["fc001.pv"]["Name"] == "fc001.pv"
    assert len(res["fc001.pv"]) == 15
    assert res["tc001.pv"]["Name"] == "tc001.pv"
    assert len(res["tc001.pv"]) == 15

    res = target_conn.read_tag_attributes(
        ["fc001.pv", "tc001.pv"], attributes=["Description"]
    )
    assert res["fc001.pv"]["Description"] == "Flow Controller"
    assert len(res["fc001.pv"]) == 2

    res = target_conn.read_tag_attributes(["fc001.pv", "tc001.pv"], attributes=["NAME"])
    assert res["fc001.pv"]["NAME"] == "fc001.pv"
    assert len(res["fc001.pv"]) == 2

    res = target_conn.read_tag_attributes(["IP_AIDef:fc001.pv", "IP_DIDef:sp001.pv"])
    assert res["fc001.pv"]["Name"] == "fc001.pv"
    assert len(res["fc001.pv"]) == 15
    assert res["sp001.pv"]["NAME"] == "sp001.pv"
    assert len(res["sp001.pv"]) == 15
