# =============================================================
#  config.py  
# =============================================================

# Stocks to track
SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "NVDA", "META", "NFLX", "AMD", "INTC", "BABA", "JPM"
]

# Kafka settings (host-side producer connects to localhost:9092)
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC   = "stock-trades"
