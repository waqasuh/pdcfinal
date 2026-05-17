"""
replay_producer.py
------------------
Reads the NYSE Daily TAQ (Trade and Quote) dataset — a genuine tick-level
trade file where a SINGLE day's data is ~3.4 GB compressed / ~14 GB uncompressed
covering every trade on NYSE, NASDAQ, and regional exchanges — and replays
the trades for the 12 tracked symbols into the Kafka topic "stock-trades".

The rest of the pipeline (Flink anomaly detection → Cassandra → Grafana)
is completely unchanged.

=============================================================================
DATASET — NYSE Daily TAQ  (FREE, no account required)
=============================================================================

  Source  : NYSE public FTP server
  URL     : https://ftp.nyse.com/Historical%20Data%20Samples/DAILY%20TAQ/
  Cost    : FREE — publicly accessible, no login needed
  Size    : Each .gz file is ~3.4 GB compressed → ~14 GB uncompressed per day
  Format  : Pipe-delimited ( | ) flat file, one row = one trade tick

  Files to download (one trading day = ~3.4 GB compressed, ~14 GB uncompressed):
    EQY_US_ALL_TRADE_20260102.gz   (~3.4 GB)
 

  Direct download link:
    https://ftp.nyse.com/Historical%20Data%20Samples/DAILY%20TAQ/EQY_US_ALL_TRADE_20260102.gz

HOW TO DOWNLOAD
---------------
Option A — Browser:
  Click each link above. The file downloads directly, no account needed.

Option B — Command line (faster):
  Windows PowerShell:
    Invoke-WebRequest -Uri "https://ftp.nyse.com/Historical%20Data%20Samples/DAILY%20TAQ/EQY_US_ALL_TRADE_20260102.gz" -OutFile "EQY_US_ALL_TRADE_20260102.gz"
    
  Linux / macOS / WSL:
    wget "https://ftp.nyse.com/Historical%20Data%20Samples/DAILY%20TAQ/EQY_US_ALL_TRADE_20260102.gz"

DO NOT unzip the files — this script reads them directly with gzip streaming,
so you never need 14 GB of disk space per file.

DAILY TAQ TRADE FILE FORMAT
----------------------------
Pipe-delimited, no header row. Columns in order:
  0  Time           — HHMMSSmmmuuu  (hour, min, sec, ms, microsec)  e.g. 093015000000
  1  Exchange       — single char   e.g. N (NYSE), Q (NASDAQ), P (Arca)
  2  Symbol         — ticker        e.g. AAPL
  3  Sale_Condition — 4 chars       e.g. "@   "
  4  Volume         — integer       e.g. 100
  5  Price          — decimal       e.g. 142.5500
  6  Trader_ID      — e.g. "D"
  7  Reporting_Party_ID
  8  Part_of_MM_ID
  9  Corr_Indicator
  10 Sequence_Number
  11 Trade_Stop_Stock_Indicator
  12 Source_Of_Trade

RUN (on Windows host, after docker-compose is up):
    python producer/replay_producer.py
"""

import gzip
import json
import os
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import KAFKA_BROKER, KAFKA_TOPIC, SYMBOLS


# ---------------------------------------------------------------------------
# Configuration — set DATA_DIR to the folder containing the .gz files
# ---------------------------------------------------------------------------

# Folder where you saved the downloaded .gz files.
# Windows example:  r"C:\Users\yourname\Downloads\taq"
# Keep the files zipped — the script reads them with gzip streaming.
DATA_DIR = r"D:\UNI\semester_6\subjects\PDC\lab\stock-anomaly"

# Files to process (add more if you downloaded additional days)
TAQ_FILES = [
    "EQY_US_ALL_TRADE_20260102.gz"
]

# Symbols to filter for (must match exactly, uppercase)
SYMBOL_SET = set(SYMBOLS)

# Messages to send per second.
# 1000  → comfortable, visible in Grafana
# 10000 → stress-test
# 0     → as fast as possible (max throughput demo)
MESSAGES_PER_SECOND = 1000

# Loop forever through the file list for continuous demo mode
LOOP_FOREVER = True

# Column indices in the pipe-delimited TAQ trade record
COL_TIME   = 0
COL_SYMBOL = 2
COL_VOLUME = 4
COL_PRICE  = 5


# ---------------------------------------------------------------------------
# Kafka setup
# ---------------------------------------------------------------------------

def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[replay] Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print("[replay] Kafka not ready, retrying in 5 s …")
            time.sleep(5)


# ---------------------------------------------------------------------------
# TAQ time string → epoch milliseconds
# Timestamp is HHMMSS followed by microseconds (6 digits), e.g. "093015123456"
# The date is embedded in the filename; we use a fixed base date per file.
# ---------------------------------------------------------------------------

def taq_time_to_epoch_ms(time_str, date_epoch_s):
    """
    time_str    : 12-char string HHMMSSuuuuuu  (hours, mins, secs, microseconds)
    date_epoch_s: Unix timestamp for midnight UTC of the trading date
    Returns epoch milliseconds.
    """
    try:
        hh = int(time_str[0:2])
        mm = int(time_str[2:4])
        ss = int(time_str[4:6])
        us = int(time_str[6:12])   # microseconds part
        seconds_into_day = hh * 3600 + mm * 60 + ss
        return int((date_epoch_s + seconds_into_day) * 1000 + us // 1000)
    except (ValueError, IndexError):
        return int(date_epoch_s * 1000)


def filename_to_date_epoch(filename):
    """
    Extract date from filename like EQY_US_ALL_TRADE_20260102.gz
    Returns Unix timestamp for midnight UTC of that date.
    """
    import datetime
    try:
        # last 8 chars before .gz is YYYYMMDD
        stem = os.path.basename(filename).replace(".gz", "")
        date_str = stem[-8:]
        dt = datetime.datetime(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]),
                               tzinfo=datetime.timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Main replay loop
# ---------------------------------------------------------------------------

def main():
    # Validate data directory
    if not os.path.isdir(DATA_DIR):
        print(
            f"\n[replay] ERROR: DATA_DIR not found: {DATA_DIR}\n"
            "\nPlease download the NYSE Daily TAQ files:\n"
            "  https://ftp.nyse.com/Historical%20Data%20Samples/DAILY%20TAQ/EQY_US_ALL_TRADE_20260102.gz\n"
            "\nThen set DATA_DIR in this file to the folder containing the .gz files.\n"
        )
        sys.exit(1)

    producer = create_kafka_producer()
    sleep_per_msg = (1.0 / MESSAGES_PER_SECOND) if MESSAGES_PER_SECOND > 0 else 0
    pass_number = 0

    while True:
        pass_number += 1
        total_sent = 0

        for gz_filename in TAQ_FILES:
            gz_path = os.path.join(DATA_DIR, gz_filename)
            if not os.path.isfile(gz_path):
                print(f"[replay] WARNING: file not found, skipping: {gz_path}")
                continue

            date_epoch_s = filename_to_date_epoch(gz_filename)
            file_size_gb = os.path.getsize(gz_path) / 1e9
            print(f"\n[replay] Pass {pass_number} — reading {gz_filename}  "
                  f"({file_size_gb:.2f} GB compressed)")

            line_count = 0
            match_count = 0

            with gzip.open(gz_path, "rt", encoding="ascii", errors="replace") as f:
                for raw_line in f:
                    line_count += 1

                    # Split on pipe
                    parts = raw_line.split("|")
                    if len(parts) < 6:
                        continue

                    symbol = parts[COL_SYMBOL].strip()
                    if symbol not in SYMBOL_SET:
                        continue

                    # Parse price and volume
                    try:
                        price  = float(parts[COL_PRICE].strip())
                        volume = float(parts[COL_VOLUME].strip())
                    except ValueError:
                        continue

                    if price <= 0:
                        continue

                    trade_time = taq_time_to_epoch_ms(
                        parts[COL_TIME].strip(), date_epoch_s
                    )

                    record = {
                        "symbol":     symbol,
                        "price":      price,
                        "volume":     volume,
                        "trade_time": trade_time,
                    }

                    producer.send(KAFKA_TOPIC, value=record)
                    match_count += 1
                    total_sent  += 1

                    # Flush every 500 matched records
                    if match_count % 500 == 0:
                        producer.flush()
                        print(f"[replay]  {match_count:,} matched | "
                              f"{line_count:,} scanned | "
                              f"last: {symbol} @ {price}")

                    if sleep_per_msg > 0:
                        time.sleep(sleep_per_msg)

            producer.flush()
            print(f"[replay] Finished {gz_filename}: "
                  f"{match_count:,} matching trades sent "
                  f"out of {line_count:,} total records scanned.")

        print(f"\n[replay] Pass {pass_number} complete — "
              f"{total_sent:,} total messages sent across all files.")

        if not LOOP_FOREVER:
            break

    print("[replay] Done.")


if __name__ == "__main__":
    main()
