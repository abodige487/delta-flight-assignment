import pandas as pd
from datetime import datetime
from io import StringIO
from task2 import most_recent_flights_csv


def write_csv(tmp_path, text: str) -> str:
    p = tmp_path / "flights.csv"
    p.write_text(text, encoding="utf-8")
    return str(p)


def test_full_timestamp_picks_latest(tmp_path):
    csv = """flightkey,flightnum,flight_dt,orig_arpt,dest_arpt,flightstatus,lastupdt
K1,100,2019-01-01,ATL,TPA,Boarding,2019-01-01 19:48:00
K1,100,2019-01-01,ATL,TPA,In,2019-01-01 19:49:00
K2,200,2019-01-02,ATL,DFW,Boarding,2019-01-02 10:00:00
"""
    path = write_csv(tmp_path, csv)
    out = most_recent_flights_csv(path)
    assert set(out["flightkey"]) == {"K1", "K2"}
    assert out.loc[out["flightkey"] == "K1", "flightstatus"].iloc[0] == "In"
    assert out.loc[out["flightkey"] == "K1", "lastupdt"].iloc[0] == pd.Timestamp("2019-01-01 19:49:00")


def test_time_only_is_attached_to_flight_dt(tmp_path):
    # lastupdt has only times → must attach to flight_dt (not today's date)
    csv = """flightkey,flightnum,flight_dt,orig_arpt,dest_arpt,flightstatus,lastupdt
K3,300,2019-01-03,ATL,ORD,Boarding,07:30:00 PM
K3,300,2019-01-03,ATL,ORD,In,08:30:00 PM
"""
    path = write_csv(tmp_path, csv)
    out = most_recent_flights_csv(path)
    assert list(out["flightkey"]) == ["K3"]
    # must be 2019-01-03 20:30:00, not today's date
    assert out["lastupdt"].iloc[0] == pd.Timestamp(datetime(2019, 1, 3, 20, 30, 0))


def test_missing_time_dropped_by_default(tmp_path):
    # One row with empty time should be dropped with default on_missing="drop"
    csv = """flightkey,flightnum,flight_dt,orig_arpt,dest_arpt,flightstatus,lastupdt
K4,400,2019-01-04,ATL,SEA,Boarding,
K4,400,2019-01-04,ATL,SEA,In,08:45:00 PM
"""
    path = write_csv(tmp_path, csv)
    out = most_recent_flights_csv(path)
    assert list(out["flightkey"]) == ["K4"]
    assert out["lastupdt"].iloc[0] == pd.Timestamp("2019-01-04 20:45:00")


def test_missing_time_midnight_policy_keeps_row(tmp_path):
    # With on_missing="midnight", blank times default to 00:00:00 and are kept
    csv = """flightkey,flightnum,flight_dt,orig_arpt,dest_arpt,flightstatus,lastupdt
K5,500,2019-01-05,ATL,RDU,Boarding,
"""
    path = write_csv(tmp_path, csv)
    out = most_recent_flights_csv(path, on_missing="midnight")
    assert list(out["flightkey"]) == ["K5"]
    assert out["lastupdt"].iloc[0] == pd.Timestamp("2019-01-05 00:00:00")


def test_duplicate_header_row_is_removed(tmp_path):
    # Simulate a second header row inside the file
    csv = """flightkey,flightnum,flight_dt,orig_arpt,dest_arpt,flightstatus,lastupdt
flightkey,flightnum,flight_dt,orig_arpt,dest_arpt,flightstatus,lastupdt
K6,600,2019-01-06,ATL,JFK,Boarding,19:00:00
"""
    path = write_csv(tmp_path, csv)
    out = most_recent_flights_csv(path)
    # Should keep only the actual data row
    assert list(out["flightkey"]) == ["K6"]
    assert out["lastupdt"].iloc[0] == pd.Timestamp("2019-01-06 19:00:00")
