from __future__ import annotations
import argparse
import re
import sys
from datetime import datetime, timedelta, time
from typing import Optional
import pandas as pd


def _parse_time_flex(s: Optional[str]):
    """
    Parse time-only strings:
        - 24h: '19:48:00', '19:48'
        - 12h: '7:48:00 PM', '7:48 PM'
    Returns datetime.time or None.
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    for fmt in ("%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


# Detect if a string includes a date component (YYYY-MM-DD, YYYY/MM/DD, or M/D/YYYY)
_DATE_RE = re.compile(r"(?:\d{4}[-/]\d{2}[-/]\d{2})|(?:\d{1,2}[-/]\d{1,2}[-/]\d{4})")


def most_recent_flights_csv(csv_path: str, on_missing: str = "drop") -> pd.DataFrame:
    """
    Converted xlsx sheet to csv and done some manual cleaning includes removing first 7 rows
    
    Read CSV and return one most-recent row per flightkey.

    Required columns:
    flightkey, flightnum, flight_dt, orig_arpt, dest_arpt, flightstatus, lastupdt

    on_missing controls rows whose lastupdt is missing/unparseable *and* lacks a date:
    - 'drop'     : drop those rows (default)
    - 'midnight' : assume 00:00:00 as time
    - 'error'    : raise ValueError
    """
    # 1) Load
    df = pd.read_csv(
    csv_path,
    dtype={
    "flightkey": "string",
    "flightnum": "string",
    "orig_arpt": "string",
        "dest_arpt": "string",
    "flightstatus": "string"
    },
    keep_default_na=False,
    )
    df.dropna(how="all", inplace=True)
    if "flightkey" in df.columns:
        fk = df["flightkey"].astype(str).str.strip()
        df = df[(fk != "") & (fk.str.lower() != "flightkey")]

    # 2) Validate
    required = ["flightkey", "flightnum", "flight_dt", "orig_arpt", "dest_arpt", "flightstatus", "lastupdt"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 3) Normalize date to midnight
    df["flight_dt"] = pd.to_datetime(df["flight_dt"], errors="raise").dt.normalize()

    # 4) Clean lastupdt and mark blanks as NA
    df["lastupdt"] = df["lastupdt"].astype(str).str.strip()
    blank_like = df["lastupdt"].isin(["", "NA", "N/A", "null", "None"])

    #handle blanks based on policy BEFORE building masks
    if on_missing == "midnight":
        # treat blanks as midnight so they join with flight_dt
        df.loc[blank_like, "lastupdt"] = "00:00:00"
    elif on_missing == "drop":
        # drop blank rows entirely
        df = df.loc[~blank_like].copy()
    elif on_missing == "error":
        if blank_like.any():
            raise ValueError("Blank 'lastupdt' values found.")

    # for non-midnight path, remaining 'lastupdt' may still be real NAs from other issues
    df.loc[df["lastupdt"].isin(["", "NA", "N/A", "null", "None"]), "lastupdt"] = pd.NA


    # 5) Decide which rows HAVE a date vs are TIME-ONLY 
    has_date = df["lastupdt"].astype("string").str.contains(_DATE_RE, na=False)
    no_date = (~has_date) & df["lastupdt"].notna()

    # 6) Parse rows that contain a date (full timestamps)
    last_with_date = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    if has_date.any():
        last_with_date.loc[has_date] = pd.to_datetime(df.loc[has_date, "lastupdt"], errors="coerce")

    # 7) Handle time-only rows: parse time and add to flight_dt
    last_from_time = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    if no_date.any():
        times = df.loc[no_date, "lastupdt"].map(_parse_time_flex)

        # Missing/unparseable time-only
        bad_mask = times.isna()
        if bad_mask.any():
            bad_vals = df.loc[no_date].loc[bad_mask, "lastupdt"].unique()[:5]
            if on_missing == "error":
                raise ValueError(f"Unparseable/missing time-only 'lastupdt' values: {bad_vals.tolist()}")
            elif on_missing == "midnight":
                times.loc[bad_mask] = time(0, 0, 0)
            else:  # 'drop'
                drop_idx = times.index[bad_mask]
                dropped = len(drop_idx)
                df = df.drop(index=drop_idx).copy()
                print(f"[info] Dropped {dropped} row(s) with missing/unparseable time-only lastupdt.", file=sys.stderr)
                # Recompute masks and containers after drop
                has_date = df["lastupdt"].astype("string").str.contains(_DATE_RE, na=False)
                no_date = (~has_date) & df["lastupdt"].notna()
                last_with_date = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
                if has_date.any():
                    last_with_date.loc[has_date] = pd.to_datetime(df.loc[has_date, "lastupdt"], errors="coerce")
                times = df.loc[no_date, "lastupdt"].map(_parse_time_flex)

        if no_date.any():
            td = times.apply(lambda t: timedelta(hours=t.hour, minutes=t.minute, seconds=t.second))
            last_from_time.loc[no_date] = df.loc[no_date, "flight_dt"] + td

    # 8) Merge parsed results into a single datetime column
    df["lastupdt_parsed"] = last_with_date
    # fill time-only where date-based is NaT (or simply where no_date True)
    df.loc[no_date, "lastupdt_parsed"] = last_from_time.loc[no_date]

    # 9) Keep the most recent row per flightkey
    df_sorted = df.sort_values(["flightkey", "lastupdt_parsed"], ascending=[True, False])
    latest = df_sorted.drop_duplicates(subset=["flightkey"], keep="first").copy()

    # 10) Final columns
    cols = ["flightkey", "flightnum", "flight_dt", "orig_arpt", "dest_arpt", "flightstatus", "lastupdt_parsed"]
    if "carrier_code" in latest.columns:
        cols.append("carrier_code")
    latest = latest[cols].rename(columns={"lastupdt_parsed": "lastupdt"}).reset_index(drop=True)
    return latest


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(description="Task 2 most recent status per flightkey")
    parser.add_argument("--input", required=True, help="Path to CSV file")
    parser.add_argument("--output", help="Optional path to write the result CSV")
    parser.add_argument(
        "--on-missing", choices=["drop", "midnight", "error"], default="drop",
        help="How to handle blank/unparseable time-only lastupdt (default: drop).",
    )
    args = parser.parse_args()

    result = most_recent_flights_csv(args.input, on_missing=args.on_missing)

    if args.output:
        result.to_csv(args.output, index=False)
        print(f"Wrote {args.output}")
    else:
        with pd.option_context("display.max_columns", None, "display.width", 140):
            print(result.to_string(index=False))


if __name__ == "__main__":
    main()
