import json
import time
import os
import numpy as np
import psycopg2
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

INPUT_TOPIC = 'price-topic'
OUTPUT_TOPIC = 'prediction-signals'

# Fenêtre d'analyse (Combien de temps on regarde en arrière pour l'IA et les Whales)
LOOKBACK_WINDOW = "1 hour" 

# Mapping Kraken -> Symboles IA
SYMBOL_MAP = {
    "XBT/USD": "BTC",
    "ETH/USD": "ETH",
    "SOL/USD": "SOL",
    "ADA/USD": "ADA",
    "DOT/USD": "DOT",
    "MATIC/USD": "MATIC",
    "LINK/USD": "LINK",
    "USDT/USD": "USDT",
    "XRP/USD": "XRP",
    "DOGE/USD": "DOGE",
    "PEPE/USD": "PEPE"
}

# --- STATE (Mémoire pour RSI) ---
# On garde les 20 derniers prix pour calculer le RSI
price_history = {} 

# --- CONNEXION DB ---
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, 
        user=DB_USER, password=DB_PASSWORD
    )

# --- CALCULATEURS ---

def calculate_rsi(prices, period=14):
    """Calcule le RSI sur une liste de prix"""
    if len(prices) < period + 1:
        return 50.0 # Pas assez de données = Neutre
    
    deltas = np.diff(prices)
    seed = deltas[:period+1]
    up = seed[seed >= 0].sum()/period
    down = -seed[seed < 0].sum()/period
    rs = up/down
    rsi = np.zeros_like(deltas)
    rsi[:period] = 100. - 100./(1. + rs)

    for i in range(period, len(prices)):
        delta = deltas[i - 1] 
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta

        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up/down
        rsi[i] = 100. - 100./(1. + rs)
        
    return rsi[-1]

def get_ai_sentiment(cursor, pair):
    """Récupère le sentiment moyen de la dernière HEURE"""
    symbol = SYMBOL_MAP.get(pair, pair.split('/')[0]) # Fallback si pas dans la map
    
    query = f"""
        SELECT AVG(sentiment_score) 
        FROM narrative_events 
        WHERE main_crypto = %s 
        AND datetime > NOW() - INTERVAL '{LOOKBACK_WINDOW}'
    """
    cursor.execute(query, (symbol,))
    result = cursor.fetchone()
    
    if result and result[0] is not None:
        raw_score = float(result[0]) # Entre -1 et 1
        # Normalisation : -1 -> 0, 0 -> 50, 1 -> 100
        return (raw_score + 1) * 50
    return 50.0 # Neutre par défaut

def get_whale_flow(cursor, pair):
    """Récupère le flux net des Whales sur la dernière HEURE"""
    query = f"""
        SELECT side, amount_usd 
        FROM whale_alerts 
        WHERE pair = %s 
        AND datetime > NOW() - INTERVAL '{LOOKBACK_WINDOW}'
    """
    cursor.execute(query, (pair,))
    rows = cursor.fetchall()
    
    if not rows:
        return 50.0
        
    buy_vol = sum(row[1] for row in rows if row[0] == 'BUY')
    sell_vol = sum(row[1] for row in rows if row[0] == 'SELL')
    
    net_flow = buy_vol - sell_vol
    total_vol = buy_vol + sell_vol
    
    if total_vol == 0:
        return 50.0
        
    # Ratio : Si tout est achat = 100, Si tout est vente = 0
    ratio = (net_flow / total_vol) # Entre -1 et 1
    return (ratio + 1) * 50

# --- MAIN ENGINE ---

def main():
    print("🔮 Prediction Engine Démarré (Window: 1h)...")
    
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Pour ne pas spammer la DB, on recalcule seulement toutes les X secondes par paire
    last_calc_time = {}

    try:
        for message in consumer:
            data = message.value
            pair = data.get('pair')
            price = float(data.get('last', 0))
            
            # 1. Mise à jour Buffer Prix (Mémoire)
            if pair not in price_history:
                price_history[pair] = deque(maxlen=30) # On garde 30 points
            price_history[pair].append(price)
            
            # Limiter la fréquence de calcul (1 calcul toutes les 5 sec max par paire)
            now = time.time()
            if now - last_calc_time.get(pair, 0) < 5:
                continue
                
            last_calc_time[pair] = now

            # 2. Calcul Technique (RSI)
            rsi_val = calculate_rsi(list(price_history[pair]))
            
            # Logique RSI inversée pour le score (RSI 30 = Achat = Score Haut)
            # Si RSI = 30 (Sura vendu) -> Score Tech = 70 (Bullish)
            # Si RSI = 70 (Suracheté) -> Score Tech = 30 (Bearish)
            tech_score = 100 - rsi_val 

            # 3. Récupération DB (IA & Whales)
            try:
                ai_score = get_ai_sentiment(cursor, pair)
                whale_score = get_whale_flow(cursor, pair)
            except Exception as e:
                print(f"⚠️ DB Err: {e}")
                conn.rollback() # Important pour ne pas bloquer la transaction
                ai_score = 50.0
                whale_score = 50.0

            # 4. Fusion (Weighted Average)
            # Tech: 40%, AI: 30%, Whales: 30%
            final_score = (tech_score * 0.4) + (ai_score * 0.3) + (whale_score * 0.3)
            
            # 5. Détermination du Signal
            signal_label = "NEUTRAL"
            if final_score >= 65: signal_label = "BUY"
            elif final_score >= 80: signal_label = "STRONG BUY"
            elif final_score <= 35: signal_label = "SELL"
            elif final_score <= 20: signal_label = "STRONG SELL"

            # 6. Envoi Kafka
            payload = {
                "type": "PREDICTION_SIGNAL",
                "pair": pair,
                "score": round(final_score, 1),
                "signal": signal_label,
                "details": {
                    "tech_rsi": round(rsi_val, 1),
                    "ai_sentiment": round(ai_score, 1),
                    "whale_flow": round(whale_score, 1)
                },
                "timestamp": time.time()
            }
            
            producer.send(OUTPUT_TOPIC, payload)
            # print(f"🔮 {pair}: {signal_label} ({final_score}) | W:{whale_score:.0f} AI:{ai_score:.0f}")

    except KeyboardInterrupt:
        print("Arrêt Prediction Engine.")
        conn.close()

if __name__ == "__main__":
    main()