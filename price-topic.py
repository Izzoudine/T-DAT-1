import json
import websocket
from kafka import KafkaProducer
import time
import threading


producer = KafkaProducer(
    bootstrap_servers = '20.199.136.163:9092',
    value_serializer=lambda v:json.dumps(v).encode('utf-8'),
    # le leader confirme l'ecriture avant de continuer
    acks=1,
    # nombre de retry
    retries=3,
    # envoie 5 messages en paralleles
    max_in_flight_requests_per_connection=5
)

WS_URL = "wss://ws.kraken.com"

kraken_top8_pairs = [
    "XBT/USD",   # BTC
    "ETH/USD",
    "USDT/USD",
    "SOL/USD",
    "ADA/USD",
    "MATIC/USD",  # not BNB
    "DOT/USD",
    "LINK/USD"
]

last_prices = {}
PRICE_CHANGE_THRESHOLD= 1.0

def send_alert(pair, alert_type, value):
    payload = {
        "pair" : pair,
        "type" : alert_type,
        "value" : value,
        "timestamp" : time.time()
    }
    try:
        producer.send("alert-topic", payload)
        producer.flush()
    except Exception as e:
        print(f"Kafka alerts error: {e}")    

def on_error(ws, error):
    print(f"WebSocket Error: {error}")
    if isinstance(error, Exception):
        print(f"Error type: {type(error).__name__}: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} - {close_msg}")

def on_open(ws):
    # abonnement ticker
    print("Entered OnOpen")
    ws.send(json.dumps({
        "event": "subscribe",
        "pair": kraken_top8_pairs,
        "subscription": {"name" : "ticker"}
    }))

    # abonnement trades
    ws.send(json.dumps({
        "event": "subscribe",
        "pair": kraken_top8_pairs,
        "subscription": {"name": "trade"}
    }))

def on_message(ws, message):
    data = json.loads(message)
    print("Entered OnMessage")
    print("DEBUG: Received data:", data)

    # Ignore heartbeat
    if isinstance(data, dict) and data.get("event") == "heartbeat":
        return

    # ------------------- raw-ticker -------------------
    if isinstance(data, list) and len(data) >= 4 and data[-2] == "ticker":
        ticker = data[1]
        pair = data[-1]

        last_price = float(ticker["c"][0])
        bid_price = float(ticker["b"][0])
        ask_price = float(ticker["a"][0])
        volume_24h = float(ticker["v"][1])
        timestamp = time.time()

        # Calcul du changement de prix
        pct_change = None
        if pair in last_prices:
            previous_price = last_prices[pair]
            pct_change = ((last_price - previous_price) / previous_price) * 100

            if abs(pct_change) >= PRICE_CHANGE_THRESHOLD:
                alert_type = "price_spike" if pct_change > 0 else "price_drop"
                send_alert(pair, alert_type, pct_change)

        # Mettre à jour le dernier prix
        last_prices[pair] = last_price

        # Préparer le payload Kafka
        payload = {
            "pair": pair,
            "last": last_price,
            "bid": bid_price,
            "ask": ask_price,
            "volume_24h": volume_24h,
            "timestamp": timestamp,
            "pct_change": round(pct_change, 2) if pct_change is not None else None
        }

        # Envoyer à Kafka
        try:
            producer.send("price-topic", payload)

            print(f"📊 {pair:9} | Last: {last_price:,.2f} USD | Change: {payload['pct_change']}")
        except Exception as e:
            print(f"❌ Kafka ticker error: {e}")

    # ------------------- raw-trades -------------------
    if isinstance(data, list) and len(data) >= 4 and data[-2] == "trade":
        pair = data[-1]
        trades = data[1]
        for trade in trades:
            payload = {
                "pair": pair,
                "price": float(trade[0]),
                "volume": float(trade[1]),
                "timestamp": float(trade[2]),
                "side": trade[3]  # "b" = buy, "s" = sell
            }
            try:
                producer.send("trade-topic", payload)
                producer.flush()

                print(f"→ {pair:9} | {payload['side']} | {payload['price']:,.2f} USD | {payload['volume']}")
            except Exception as e:
                print(f"❌ Kafka trades error: {e}")


def periodic_flush():
    while True:
        time.sleep(1)
        try:
            producer.flush(timeout=10)
        except Exception as e:
            print("Flush error:", e)

threading.Thread(target=periodic_flush, daemon=True).start()

if __name__ == "__main__":
    print("Starting Kraken WebSocket → Kafka bridge...")

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    # THIS IS THE KEY: automatic reconnection + ping
    ws.run_forever(
        ping_interval=30,  
        ping_timeout=10,
        reconnect=5            
    )


