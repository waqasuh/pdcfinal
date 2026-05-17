-- =============================================================
--  schema.cql  —  run once after Cassandra starts
-- =============================================================

CREATE KEYSPACE IF NOT EXISTS stock_market
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
  };

USE stock_market;

-- ---------------------------------------------------------------
-- Table 1: every trade tick received from Finnhub
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    symbol      text,
    trade_time  timestamp,
    price       double,
    volume      double,
    PRIMARY KEY (symbol, trade_time)
) WITH CLUSTERING ORDER BY (trade_time DESC)
  AND default_time_to_live = 86400;   -- keep 24 h of raw ticks

-- ---------------------------------------------------------------
-- Table 2: anomalies detected by the Flink job
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomalies (
    symbol        text,
    detected_at   timestamp,
    anomaly_type  text,
    price         double,
    volume        double,
    PRIMARY KEY (symbol, detected_at)
) WITH CLUSTERING ORDER BY (detected_at DESC)
  AND default_time_to_live = 604800;  -- keep 7 days of anomalies
