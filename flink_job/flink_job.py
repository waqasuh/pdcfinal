"""
flink_job.py
------------
Runs INSIDE the Flink Docker container (submitted via docker exec).

Reads trade messages from Kafka topic "stock-trades", groups them into
10-second tumbling windows per symbol, detects three anomaly types, and
writes results to both Cassandra tables:
  - stock_market.trades
  - stock_market.anomalies

This file is intentionally written in plain Python using the kafka-python
consumer and cassandra-driver directly (no PyFlink API) so that:
  • It is easy to read for a university course.
  • It works reliably inside the plain flink:1.17 image via `python3`.
  • It requires only two pip packages: kafka-python + cassandra-driver.

Anomaly thresholds (simple, no ML):
  volume_spike  → window_volume  > 3 × mean of last 5 window volumes
  flash_crash   → price dropped  > 2 % within the window
  price_spike   → price rose     > 2 % within the window
"""

import json
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy


# ---------------------------------------------------------------------------
# Configuration (all resolved inside Docker network)
# ---------------------------------------------------------------------------

KAFKA_BROKER  = "kafka:29092"
KAFKA_TOPIC   = "stock-trades"
KAFKA_GROUP   = "flink-anomaly-group"

CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE       = "stock_market"

WINDOW_SECONDS = 10          # tumbling window size
HISTORY_WINDOWS = 5          # how many past windows to average for volume baseline
VOLUME_SPIKE_MULTIPLIER = 3  # current window volume > 3× mean → spike
PRICE_CHANGE_THRESHOLD  = 0.02  # 2 %


# ---------------------------------------------------------------------------
# In-memory window state  (simple dicts — no Flink state API)
# ---------------------------------------------------------------------------

# Accumulates raw ticks in the current open window for each symbol
current_window = defaultdict(list)   # symbol → [{"price": ..., "volume": ...}, ...]
window_start   = {}                  # symbol → float (epoch seconds)

# Rolling history of total volumes from the last N closed windows
volume_history = defaultdict(list)   # symbol → [vol1, vol2, …]  (max HISTORY_WINDOWS entries)


# ---------------------------------------------------------------------------
# Cassandra helpers
# ---------------------------------------------------------------------------

def connect_cassandra():
    """Retry Cassandra connection until the node is ready."""
    while True:
        try:
            cluster = Cluster(
                [CASSANDRA_HOST],
                port=CASSANDRA_PORT,
                default_retry_policy=RetryPolicy(),
            )
            session = cluster.connect(KEYSPACE)
            print("[flink_job] Connected to Cassandra")
            return session
        except Exception as exc:
            print(f"[flink_job] Cassandra not ready ({exc}), retrying in 5 s …")
            time.sleep(5)


def insert_trade(session, symbol, price, volume, trade_time_ms):
    """Write a single trade tick to cassandra stock_market.trades."""
    # trade_time_ms is epoch milliseconds from Finnhub
    ts = datetime.fromtimestamp(trade_time_ms / 1000.0, tz=timezone.utc)
    session.execute(
        """
        INSERT INTO trades (symbol, trade_time, price, volume)
        VALUES (%s, %s, %s, %s)
        """,
        (symbol, ts, price, volume),
    )


def insert_anomaly(session, symbol, anomaly_type, price, volume):
    """Write an anomaly record to cassandra stock_market.anomalies."""
    now = datetime.now(tz=timezone.utc)
    session.execute(
        """
        INSERT INTO anomalies (symbol, detected_at, anomaly_type, price, volume)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (symbol, now, anomaly_type, price, volume),
    )
    print(f"[flink_job] ANOMALY → {symbol}  type={anomaly_type}  "
          f"price={price:.4f}  vol={volume:.2f}")


# ---------------------------------------------------------------------------
# Anomaly detection (pure functions, no ML)
# ---------------------------------------------------------------------------

def detect_anomalies(symbol, ticks, session):
    """
    Given a list of ticks for one closed window, compute window statistics
    and check all three anomaly conditions.

    ticks: list of {"price": float, "volume": float}
    """
    if not ticks:
        return

    prices  = [t["price"]  for t in ticks]
    volumes = [t["volume"] for t in ticks]

    min_price    = min(prices)
    max_price    = max(prices)
    first_price  = prices[0]
    last_price   = prices[-1]
    total_volume = sum(volumes)
    avg_price    = (min_price + max_price) / 2.0

    # --- volume spike ---
    hist = volume_history[symbol]
    if len(hist) >= 1:                          # need at least one past window
        mean_vol = sum(hist) / len(hist)
        if mean_vol > 0 and total_volume > VOLUME_SPIKE_MULTIPLIER * mean_vol:
            insert_anomaly(session, symbol, "volume_spike", avg_price, total_volume)

    # Update volume history (keep only last HISTORY_WINDOWS entries)
    hist.append(total_volume)
    if len(hist) > HISTORY_WINDOWS:
        volume_history[symbol] = hist[-HISTORY_WINDOWS:]

    # --- flash crash  (price dropped > 2 % from first to last tick) ---
    if first_price > 0:
        change = (last_price - first_price) / first_price
        if change < -PRICE_CHANGE_THRESHOLD:
            insert_anomaly(session, symbol, "flash_crash", last_price, total_volume)

        # --- price spike (price rose > 2 %) ---
        if change > PRICE_CHANGE_THRESHOLD:
            insert_anomaly(session, symbol, "price_spike", last_price, total_volume)


# ---------------------------------------------------------------------------
# Kafka consumer setup
# ---------------------------------------------------------------------------

def create_consumer():
    """Connect to Kafka with retries."""
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            print(f"[flink_job] Connected to Kafka at {KAFKA_BROKER}, "
                  f"topic={KAFKA_TOPIC}")
            return consumer
        except NoBrokersAvailable:
            print("[flink_job] Kafka not ready, retrying in 5 s …")
            time.sleep(5)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    print("[flink_job] Starting …")
    session  = connect_cassandra()
    consumer = create_consumer()

    print(f"[flink_job] Processing trades with {WINDOW_SECONDS}s tumbling windows …")

    for message in consumer:
        trade = message.value

        symbol     = trade.get("symbol")
        price      = trade.get("price")
        volume     = trade.get("volume")
        trade_time = trade.get("trade_time")   # epoch ms

        if not symbol or price is None or price <= 0:
            continue

        now = time.time()   # epoch seconds

        # ---- initialise window start time for new symbols ----
        if symbol not in window_start:
            window_start[symbol] = now

        # ---- check if current window has expired ----
        elapsed = now - window_start[symbol]
        if elapsed >= WINDOW_SECONDS:
            # Close the window: run anomaly detection on accumulated ticks
            detect_anomalies(symbol, current_window[symbol], session)
            # Reset for the new window
            current_window[symbol] = []
            window_start[symbol]   = now

        # ---- accumulate tick into current open window ----
        current_window[symbol].append({"price": price, "volume": volume})

        # ---- always write every trade tick to Cassandra trades table ----
        try:
            if trade_time:
                insert_trade(session, symbol, price, volume, trade_time)
        except Exception as exc:
            print(f"[flink_job] trade insert error: {exc}")


if __name__ == "__main__":
    main()
