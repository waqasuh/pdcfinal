# Real-Time Stock Market Anomaly Detection

A parallel & distributed computing project that streams real NYSE/NASDAQ trade ticks
through **Apache Kafka**, detects anomalies in **Apache Flink**, stores results in
**Apache Cassandra**, and visualises everything in **Grafana** — running entirely on
your Windows laptop via Docker Desktop.

The data source is the **NYSE Daily TAQ** (Trade and Quote) dataset — a free,
publicly available tick-level file where a **single day of trading is ~3.4 GB
compressed and ~14 GB uncompressed**, covering every trade executed across NYSE,
NASDAQ, and all US regional exchanges.

---

## Architecture

```
NYSE Daily TAQ dataset  (free, public, ~3.4 GB per day compressed)
        │
        ▼
  replay_producer.py    ← runs on your Windows host
  Reads .gz files with gzip streaming (no need to unzip)
  Filters for 12 symbols, sends each trade tick to Kafka
        │   Kafka topic: stock-trades
        ▼
  Apache Kafka          ← Docker container
        │
        ▼
  flink_job.py          ← runs inside Flink Docker container
  10-second tumbling windows per symbol
  Detects: volume_spike | flash_crash | price_spike
        │
        ▼
  Apache Cassandra      ← Docker container
  ├── stock_market.trades      every trade tick
  └── stock_market.anomalies   detected anomalies
        │
        ▼
  Grafana               ← Docker container  →  http://localhost:3000
```

---

## Prerequisites

| Tool                  | Version             | Where to get it                                 |
| --------------------- | ------------------- | ----------------------------------------------- |
| Docker Desktop        | Latest              | https://www.docker.com/products/docker-desktop/ |
| Python                | 3.9 or 3.10         | https://www.python.org/downloads/               |
| pip                   | bundled with Python | —                                              |
| ~7 GB free disk space | for 2 TAQ .gz files | —                                              |

> **Windows note:** Docker Desktop must be running before any `docker` command.
> Enable the WSL 2 backend when Docker Desktop prompts — it makes containers faster.

---

## Folder Structure

```
stock-anomaly/
├── docker-compose.yml          ← all Docker services
|__ EQY_US_ALL_TRADE_20260102.gz ← dataset
|
├── requirements.txt            ← Python packages for Windows host
├── config.py                   ← symbol list + Kafka settings
├── producer/
│   └── replay_producer.py      ← NYSE TAQ .gz → Kafka
├── flink_job/
│   └── flink_job.py            ← Kafka → anomaly detection → Cassandra
├── cassandra/
│   └── schema.cql              ← keyspace + two tables
└── README.md                   ← this file
```

---

## Part 1 — Download the NYSE Daily TAQ Dataset

### What it is

The **NYSE Daily TAQ (Trade and Quote)** file is the official consolidated tape
of every trade executed across all US equity exchanges on a given trading day.
A single file covers NYSE, NASDAQ, NYSE Arca, NYSE American, and regional exchanges.

| Field                       | Detail                                                    |
| --------------------------- | --------------------------------------------------------- |
| **Source**            | NYSE public FTP server                                    |
| **Cost**              | **Free — no account, no login required**           |
| **Compressed size**   | ~3.4 GB per day                                           |
| **Uncompressed size** | ~14 GB per day                                            |
| **Records**           | ~150–200 million trade ticks per day                     |
| **Format**            | Pipe-delimited (`\|`) flat file, one row = one trade     |
| **Columns**           | Time, Exchange, Symbol, Sale_Condition, Volume, Price, … |

### Files to download

Download both files below — together they give you ~7 GB compressed / ~28 GB
worth of real tick data, far exceeding the 2 GB requirement.

| File                             | Direct download link                                                                      |
| -------------------------------- | ----------------------------------------------------------------------------------------- |
| `EQY_US_ALL_TRADE_20260102.gz` | https://ftp.nyse.com/Historical%20Data%20Samples/DAILY%20TAQ/EQY_US_ALL_TRADE_20260102.gz |
|                                  |                                                                                           |

> **Do NOT unzip the files.** `replay_producer.py` reads them using Python's built-in
> `gzip` module with streaming — you never need the 14 GB of uncompressed space.
> The .gz files themselves are the dataset.

### How to download — Option A (browser)

Click on the link above. The file downloads directly from NYSE's public FTP server.
No account, no form, no login.


### After downloading

Open `producer/replay_producer.py` and update `DATA_DIR` to the folder where
you saved the .gz files:

```python
# Line ~75 in replay_producer.py — change this path
DATA_DIR = r"C:\taq-data"
```

---

## Part 2 — Full Setup and Run Guide

### Step 1 — Install Python packages (Windows host)

Open a terminal (`cmd` or PowerShell) in the `stock-anomaly/` folder:

```bash
pip install -r requirements.txt
```

---

### Step 2 — Start all Docker services

```bash
docker-compose up -d
```

This starts: Zookeeper, Kafka, Flink JobManager, Flink TaskManager, Cassandra, Grafana.

Wait **60–90 seconds** for everything to initialise. Cassandra is the slowest.

Verify all 6 containers are running:

```bash
docker ps
```

Every container should show status `Up`. If any show `Restarting`, wait 30 more
seconds and run `docker ps` again.

---

### Step 3 — Apply the Cassandra schema

Wait until Cassandra shows `(healthy)` in `docker ps`, then run:

```bash
docker exec -i cassandra cqlsh < cassandra/schema.cql
```

Verify both tables were created:

```bash
docker exec -it cassandra cqlsh -e "DESCRIBE stock_market;"
```

You should see `trades` and `anomalies` listed.

---

### Step 4 — Install Python packages inside the Flink container

```bash
docker exec flink-jobmanager pip3 install kafka-python cassandra-driver
```

Takes 1–2 minutes. Only needed once per `docker-compose up`.

---

### Step 5 — Start the Flink job (inside the container)

Run in the background:

```bash
docker exec -d flink-jobmanager python3 /opt/flink_job/flink_job.py
```

Or watch its output live in a dedicated terminal:

```bash
docker exec -it flink-jobmanager python3 /opt/flink_job/flink_job.py
```

Confirm it started correctly:

```bash
docker logs flink-jobmanager 2>&1 | tail -20
```

Expected output:

```
[flink_job] Connected to Cassandra
[flink_job] Connected to Kafka at kafka:29092, topic=stock-trades
[flink_job] Processing trades with 10s tumbling windows …
```

---

### Step 6 — Start the replay producer

Make sure `DATA_DIR` in `replay_producer.py` is set to your .gz folder, then:

```bash
python producer/replay_producer.py
```

Expected output:

```
[replay] Connected to Kafka at localhost:9092

[replay] Pass 1 — reading EQY_US_ALL_TRADE_20260102.gz  (3.41 GB compressed)
[replay]  500 matched | 1,842,301 scanned | last: AAPL @ 142.55
[replay]  1,000 matched | 3,204,887 scanned | last: MSFT @ 234.12
[replay]  1,500 matched | 4,901,024 scanned | last: NVDA @ 123.45
...
[replay] Finished EQY_US_ALL_TRADE_20260102.gz: 94,217 matching trades sent
         out of 187,432,018 total records scanned.

...
```

> The script streams through ~187 million rows per file to extract the ~94,000
> trades for your 12 symbols. This is the "big data" processing — Kafka, Flink,
> and Cassandra are handling a genuine production-scale dataset.

#### Replay speed settings (in replay_producer.py)

| Setting                         | Effect                                           |
| ------------------------------- | ------------------------------------------------ |
| `MESSAGES_PER_SECOND = 100`   | Slow — easy to watch panel-by-panel in Grafana  |
| `MESSAGES_PER_SECOND = 1000`  | Default — good balance for a demo               |
| `MESSAGES_PER_SECOND = 10000` | Fast — maximum pipeline stress test             |
| `MESSAGES_PER_SECOND = 0`     | Unlimited — as fast as the machine allows       |
| `LOOP_FOREVER = True`         | Keeps cycling through files for continuous demos |
| `LOOP_FOREVER = False`        | Stops after one complete pass                    |

---

## Part 3 — Verifying Each Component

### Kafka — confirm messages are flowing

```bash
docker exec -it kafka kafka-console-consumer ^
  --bootstrap-server kafka:29092 ^
  --topic stock-trades ^
  --from-beginning ^
  --max-messages 5
```

Expected:

```json
{"symbol": "AAPL", "price": 142.55, "volume": 100.0, "trade_time": 1664793015000}
{"symbol": "MSFT", "price": 234.12, "volume": 300.0, "trade_time": 1664793016000}
```

### Cassandra — confirm data is being written

```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM stock_market.trades LIMIT 10;"
```

```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM stock_market.anomalies LIMIT 10;"
```

### Flink — confirm the job is running

Open http://localhost:8081 — Flink Web UI.

> The job does not appear as a Flink "job" in the UI because it runs as a plain
> Python script rather than a JAR submission. The Flink container is still the
> isolated execution environment — this is intentional for readability.

### Grafana — open the dashboard

Open http://localhost:3000 · Username: `admin` · Password: `admin`

---

## Part 4 — Grafana Dashboard Setup

### A — Add the Cassandra data source

1. Go to **Connections → Data Sources → Add data source**
2. Search for **Cassandra** → select **HadesArchitect Cassandra**
3. Fill in:
   - **Host:** `cassandra`
   - **Port:** `9042`
   - **Keyspace:** `stock_market`
   - **Consistency:** `LOCAL_ONE`
   - Leave username and password blank
4. Click **Save & Test** → should show "Data source connected"

---

### B — Create the three panels

Go to **Dashboards → New → New Dashboard → Add visualization**

#### Panel 1 — Trade prices per symbol (time series)

- **Visualization:** Time series
- **Data source:** Cassandra
- **Query (CQL):**

```cql
SELECT trade_time, symbol, price
FROM stock_market.trades
WHERE trade_time > $__timeFrom AND trade_time < $__timeTo
ALLOW FILTERING
```

- **Column mapping:** Time = `trade_time` · Metric = `symbol` · Value = `price`
- **Title:** `Trade Prices`
- Click **Apply**

#### Panel 2 — Volume per symbol (bar chart)

- **Visualization:** Bar chart
- **Data source:** Cassandra
- **Query (CQL):**

```cql
SELECT symbol, SUM(volume) AS total_volume
FROM stock_market.trades
WHERE trade_time > $__timeFrom AND trade_time < $__timeTo
GROUP BY symbol
ALLOW FILTERING
```

- **Column mapping:** Label = `symbol` · Value = `total_volume`
- **Title:** `Volume per Symbol`
- Click **Apply**

#### Panel 3 — Anomalies table

- **Visualization:** Table
- **Data source:** Cassandra
- **Query (CQL):**

```cql
SELECT detected_at, symbol, anomaly_type, price, volume
FROM stock_market.anomalies
WHERE detected_at > $__timeFrom AND detected_at < $__timeTo
ALLOW FILTERING
```

- **Title:** `Detected Anomalies`
- Click **Apply**

---

### C — Configure auto-refresh

- Set the time range (top right) to **Last 1 hour**
- Set refresh interval to **5s**
- Click **Save dashboard** → name it `Stock Anomaly Monitor`

---

## Part 5 — Stopping Everything

Stop the producer: press `Ctrl+C` in its terminal.

Stop Docker services (keeps Cassandra data):

```bash
docker-compose down
```

Full reset (deletes all stored data):

```bash
docker-compose down -v
```

---

## Reference

### Anomaly detection logic

Every 10 seconds, for each symbol, the Flink job closes the window and checks:

| Anomaly type     | Condition                                                          |
| ---------------- | ------------------------------------------------------------------ |
| `volume_spike` | Window total volume > 3× the average of the last 5 closed windows |
| `flash_crash`  | Last price in window dropped > 2% vs first price in window         |
| `price_spike`  | Last price in window rose > 2% vs first price in window            |

When any condition is true a row is inserted into `stock_market.anomalies`.

---

### Tracked symbols

AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META, NFLX, AMD, INTC, BABA, JPM

---

### Port reference

| Service                       | Address               |
| ----------------------------- | --------------------- |
| Kafka (from Windows host)     | `localhost:9092`    |
| Kafka (inside Docker network) | `kafka:29092`       |
| Flink Web UI                  | http://localhost:8081 |
| Cassandra                     | `localhost:9042`    |
| Grafana                       | http://localhost:3000 |

---

### Troubleshooting

| Problem                                         | Fix                                                                                           |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Container shows `Restarting` in `docker ps` | Wait 60 s more; Cassandra is slow to start                                                    |
| Flink job prints "Cassandra not ready"          | Normal — retries automatically every 5 s                                                     |
| Producer prints "Kafka not ready"               | Normal — retries automatically every 5 s                                                     |
| `cqlsh` schema command fails                  | Cassandra not healthy yet — wait for `(healthy)` in `docker ps`                          |
| DATA_DIR not found error                        | Check the path in `replay_producer.py`; must point to the folder with .gz files             |
| Download fails / connection refused             | NYSE FTP can be slow; retry or use a VPN if blocked in your region                            |
| 0 matching trades found                         | Symbol names in TAQ are uppercase with no spaces; GOOGL appears as "GOOGL" — already handled |
| Anomalies table stays empty                     | Need ≥ 2 closed 10-second windows per symbol; let the producer run 30+ seconds first         |
| Grafana plugin not found                        | Run `docker-compose down` then `docker-compose up -d`, wait 2 min for plugin install      |
| Port 9092 already in use                        | Stop any other Kafka instance; or change the host port in `docker-compose.yml`              |
