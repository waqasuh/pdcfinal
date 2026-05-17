"""
setup_grafana.py
----------------
Provisions the Grafana Stock Anomaly Monitor dashboard via API.

Usage:
    pip install requests
    python setup_grafana.py
"""

import json
import sys
import time
import requests

GRAFANA_URL      = "http://localhost:3000"
GRAFANA_USER     = "admin"
GRAFANA_PASSWORD = "admin"

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "INTC", "BABA", "JPM"]
REF_IDS = "ABCDEFGHIJKL"
LIMIT   = 500

session = requests.Session()
session.auth = (GRAFANA_USER, GRAFANA_PASSWORD)
session.headers.update({"Content-Type": "application/json"})


def get_datasource():
    resp = session.get(f"{GRAFANA_URL}/api/datasources")
    resp.raise_for_status()
    for ds in resp.json():
        if "cassandra" in ds.get("type", "").lower() or "cassandra" in ds.get("name", "").lower():
            print(f"[setup] Datasource: {ds['name']} uid={ds['uid']} id={ds['id']}")
            return ds["uid"], ds["id"]
    print("[setup] ERROR: Cassandra datasource not found.")
    sys.exit(1)


def make_trades_target(ds_uid, ds_id, symbol, ref_id, value_col):
    return {
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": ds_uid},
        "alias": symbol,
        "columnId": "",           # empty — prevents splitting into per-row series
        "columnTime": "trade_time",
        "columnValue": value_col,
        "datasourceId": ds_id,
        "filtering": True,
        "keyspace": "stock_market",
        "table": "trades",
        "queryType": "query",
        "rawQuery": True,
        "refId": ref_id,
        # ORDER BY ASC so data plots left-to-right correctly
        "target": f"SELECT trade_time, {value_col} FROM stock_market.trades WHERE symbol = '{symbol}' ORDER BY trade_time ASC LIMIT {LIMIT};"
    }


def make_anomaly_target(ds_uid, ds_id, symbol, ref_id):
    return {
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": ds_uid},
        "alias": symbol,
        "columnId": "",
        "columnTime": "detected_at",
        "columnValue": "price",
        "datasourceId": ds_id,
        "filtering": True,
        "keyspace": "stock_market",
        "table": "anomalies",
        "queryType": "query",
        "rawQuery": True,
        "refId": ref_id,
        "target": f"SELECT detected_at, symbol, anomaly_type, price, volume FROM stock_market.anomalies WHERE symbol = '{symbol}' LIMIT 100;"
    }


def ts_field_config(unit="short", draw_style="line", fill=0):
    return {
        "defaults": {
            "color": {"mode": "palette-classic"},
            "custom": {
                "drawStyle": draw_style, "lineInterpolation": "linear",
                "barAlignment": 0, "lineWidth": 1, "fillOpacity": fill,
                "gradientMode": "none", "spanNulls": False, "showPoints": "auto",
                "pointSize": 5, "stacking": {"mode": "none", "group": "A"},
                "axisPlacement": "auto", "axisLabel": "", "axisColorMode": "text",
                "scaleDistribution": {"type": "linear"}, "axisCenteredZero": False,
                "hideFrom": {"tooltip": False, "viz": False, "legend": False},
                "thresholdsStyle": {"mode": "off"}
            },
            "mappings": [],
            "thresholds": {"mode": "absolute", "steps": [
                {"color": "green", "value": None}, {"color": "red", "value": 80}
            ]},
            "unit": unit
        },
        "overrides": []
    }


def make_price_panel(ds_uid, ds_id):
    return {
        "id": 1, "type": "timeseries",
        "title": "Trade Prices per Symbol",
        "gridPos": {"h": 10, "w": 24, "x": 0, "y": 0},
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": ds_uid},
        "targets": [make_trades_target(ds_uid, ds_id, s, REF_IDS[i], "price")
                    for i, s in enumerate(SYMBOLS)],
        "fieldConfig": ts_field_config(unit="currencyUSD", fill=0),
        "options": {
            "tooltip": {"mode": "multi", "sort": "none"},
            "legend": {"showLegend": True, "displayMode": "table", "placement": "right", "calcs": ["last"]}
        }
    }


def make_volume_panel(ds_uid, ds_id):
    return {
        "id": 2, "type": "timeseries",
        "title": "Volume per Symbol",
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 10},
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": ds_uid},
        "targets": [make_trades_target(ds_uid, ds_id, s, REF_IDS[i], "volume")
                    for i, s in enumerate(SYMBOLS)],
        "fieldConfig": ts_field_config(unit="short", draw_style="bars", fill=10),
        "options": {
            "tooltip": {"mode": "multi", "sort": "none"},
            "legend": {"showLegend": True, "displayMode": "table", "placement": "right", "calcs": ["last"]}
        }
    }


def make_anomalies_panel(ds_uid, ds_id):
    return {
        "id": 3, "type": "table",
        "title": "Detected Anomalies",
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 18},
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": ds_uid},
        "targets": [make_anomaly_target(ds_uid, ds_id, s, REF_IDS[i])
                    for i, s in enumerate(SYMBOLS)],
        "pluginVersion": "10.0.0",
        "fieldConfig": {
            "defaults": {
                "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [
                    {"color": "green", "value": None}, {"color": "red", "value": 80}
                ]}
            },
            "overrides": []
        },
        "options": {
            "cellHeight": "sm",
            "footer": {"countRows": False, "fields": "", "reducer": ["sum"], "show": False},
            "showHeader": True,
            "sortBy": [{"desc": True, "displayName": "detected_at"}]
        }
    }


def create_dashboard(ds_uid, ds_id):
    dashboard = {
        "id": None,
        "uid": "stock-anomaly-monitor",
        "title": "Stock Anomaly Monitor",
        "tags": ["stocks", "kafka", "flink", "cassandra"],
        "style": "dark",
        "timezone": "utc",
        "schemaVersion": 38,
        "version": 1,
        "refresh": "off",
        "time": {"from": "2026-01-02T09:00:00.000Z", "to": "2026-01-02T21:00:00.000Z"},
        "timepicker": {},
        "templating": {"list": []},
        "annotations": {"list": [{
            "builtIn": 1,
            "datasource": {"type": "grafana", "uid": "-- Grafana --"},
            "enable": True, "hide": True,
            "iconColor": "rgba(0, 211, 255, 1)",
            "name": "Annotations & Alerts", "type": "dashboard"
        }]},
        "panels": [
            make_price_panel(ds_uid, ds_id),
            make_volume_panel(ds_uid, ds_id),
            make_anomalies_panel(ds_uid, ds_id),
        ]
    }

    payload = {"dashboard": dashboard, "overwrite": True, "message": "Provisioned by setup_grafana.py"}
    resp = session.post(f"{GRAFANA_URL}/api/dashboards/db", data=json.dumps(payload))
    resp.raise_for_status()
    url = resp.json().get("url", "/d/stock-anomaly-monitor")
    print(f"[setup] Dashboard ready: {GRAFANA_URL}{url}")


def main():
    print(f"[setup] Connecting to Grafana at {GRAFANA_URL} ...")
    for attempt in range(10):
        try:
            if session.get(f"{GRAFANA_URL}/api/health").status_code == 200:
                print("[setup] Grafana is ready.")
                break
        except requests.exceptions.ConnectionError:
            pass
        print(f"[setup] Waiting ({attempt+1}/10)...")
        time.sleep(3)
    else:
        print("[setup] ERROR: Cannot reach Grafana.")
        sys.exit(1)

    ds_uid, ds_id = get_datasource()
    create_dashboard(ds_uid, ds_id)

    print("\n[setup] Done.")
    print(f"        {GRAFANA_URL}/d/stock-anomaly-monitor/stock-anomaly-monitor")
    print("\n[setup] NOTE: Trades data is from 2026-01-02.")
    print("        Anomalies are from today — those panels may show empty until")
    print("        you change their time range to 'Last 6 hours'.")


if __name__ == "__main__":
    main()