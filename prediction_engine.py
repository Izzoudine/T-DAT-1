import json
import time
import os
import pandas as pd
import pandas_ta as ta
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')

INPUT_TOPICS = ['narrative-events', 'analytics-updates']
OUTPUT_TOPIC = 'prediction-signals'

# --- TES 8 PAIRES KRAKEN ---
TARGET_PAIRS = [
    "XBT/USD", "ETH/USD", "USDT/USD", "SOL/USD",
    "ADA/USD", "MATIC/USD", "DOT/USD", "LINK/USD"
]

# --- MAPPING INTELLIGENT (News -> Kraken) ---
# L'IA peut dire "BTC", "Bitcoin", "Polygon"... on traduit tout en paires Kraken.
SYMBOL_MAP = {
    "BTC": "XBT/USD", "BITCOIN": "XBT/USD", "XBT": "XBT/USD",
    "ETH": "ETH/USD", "ETHEREUM": "ETH/USD",
    "SOL": "SOL/USD", "SOLANA": "SOL/USD",
    "ADA": "ADA/USD", "CARDANO": "ADA/USD",
    "MATIC": "MATIC/USD", "POLYGON": "MATIC/USD",
    "DOT": "DOT/USD", "POLKADOT": "DOT/USD",
    "LINK": "LINK/USD", "CHAINLINK": "LINK/USD",
    "USDT": "USDT/USD", "TETHER": "USDT/USD"
}

# --- STATE (Scores en Mémoire) ---
# On initialise tout à 0 (Neutre) pour tes 8 paires
latest_sentiment = {pair: 50 for pair in TARGET_PAIRS} # 50 = Neutre
whale_pressure = {pair: 0 for pair in TARGET_PAIRS}    # 0 = Pas de pression

# --- CONNEXION DB ---
def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

# --- ANALYSE TECHNIQUE (RSI) ---
def get_technical_score(pair):
    """Calcule le RSI depuis la DB"""
    # Cas spécial USDT (Stablecoin) : Pas de RSI pertinent, on renvoie Neutre
    if pair == "USDT/USD":
        return 50

    try:
        conn = get_db_connection()
        # On prend les 60 dernières bougies (1 heure)
        query = f"""
            SELECT price FROM crypto_prices 
            WHERE pair = '{pair}' 
            ORDER BY datetime DESC LIMIT 60
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        if len(df) < 14: return 50
        
        df = df.iloc[::-1] # On remet dans l'ordre chronologique
        
        # Calcul RSI 14
        rsi = ta.rsi(df['price'], length=14).iloc[-1]
        
        # Logique RSI inverse :
        # RSI < 30 (Survendu) -> Score BULLISH (80-100)
        # RSI > 70 (Suracheté) -> Score BEARISH (0-20)
        if rsi < 30: return 90
        if rsi < 40: return 70
        if rsi > 70: return 10
        if rsi > 60: return 30
        return 50
        
    except Exception as e:
        # Si erreur (ex: pas assez de données), on reste neutre
        return 50

# --- MAIN ---
def main():
    print(f"🔮 Prediction Engine sur 8 Paires: {TARGET_PAIRS}")
    
    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    last_prediction_time = time.time()

    for message in consumer:
        topic = message.topic
        data = message.value
        
        # -----------------------------
        # 1. MISE À JOUR DES DONNÉES
        # -----------------------------

        # A. Si c'est une News (Narrative)
        if topic == 'narrative-events':
            raw_symbol = data.get('main_crypto', '').upper()
            
            # On utilise le MAP pour trouver la paire Kraken correspondante
            pair = SYMBOL_MAP.get(raw_symbol)
            
            if pair and pair in TARGET_PAIRS:
                # Score IA: de -1 à 1. On transforme en 0 à 100.
                raw_score = data.get('sentiment_score', 0)
                sentiment_val = (raw_score + 1) * 50
                
                latest_sentiment[pair] = sentiment_val
                print(f"📰 News pour {pair} ({raw_symbol}): Score IA {sentiment_val:.0f}/100")

        # B. Si c'est une Whale
        elif topic == 'analytics-updates' and data.get('type') == 'WHALE_ALERT':
            pair = data.get('pair')
            
            if pair in TARGET_PAIRS:
                side = data.get('side')
                # +20 si Achat, -20 si Vente
                impact = 20 if side == 'BUY' else -20
                
                whale_pressure[pair] += impact
                # On borne le score whale entre -40 et +40
                whale_pressure[pair] = max(min(whale_pressure[pair], 40), -40)
                print(f"🐋 Whale sur {pair}: Pression ajustée à {whale_pressure[pair]}")

        # -----------------------------
        # 2. CALCUL & PRÉDICTION (Toutes les 5 sec)
        # -----------------------------
        if time.time() - last_prediction_time > 5:
            
            # On réduit doucement la pression des whales (l'effet s'estompe)
            for p in whale_pressure:
                whale_pressure[p] *= 0.95 

            # On boucle sur TES 8 PAIRES
            for pair in TARGET_PAIRS:
                
                # A. Technique (40%)
                tech_score = get_technical_score(pair)
                
                # B. Sentiment IA (30%)
                sent_score = latest_sentiment.get(pair, 50)
                
                # C. Whales (30%) -> Base 50 + Pression (-40 à +40)
                whale_score = 50 + whale_pressure.get(pair, 0)
                
                # FORMULE FINALE
                final_score = (tech_score * 0.4) + (whale_score * 0.3) + (sent_score * 0.3)
                
                # Interprétation Signal
                signal = "NEUTRAL"
                if final_score >= 70: signal = "BUY"
                if final_score >= 85: signal = "STRONG_BUY"
                if final_score <= 30: signal = "SELL"
                if final_score <= 15: signal = "STRONG_SELL"
                
                # Envoi Kafka (Uniquement si ce n'est pas USDT, car USDT ne pump pas)
                if pair != "USDT/USD":
                    payload = {
                        "type": "PREDICTION_SIGNAL",
                        "pair": pair,
                        "score": round(final_score, 1),
                        "signal": signal,
                        "details": {
                            "tech_rsi": tech_score,
                            "ai_sentiment": round(sent_score, 1),
                            "whale_flow": round(whale_score, 1)
                        },
                        "timestamp": time.time()
                    }
                    producer.send(OUTPUT_TOPIC, payload)
                    
                    # Petit log pour voir que ça vit
                    if signal != "NEUTRAL":
                        print(f"🔮 {pair} -> {signal} ({final_score:.1f})")

            last_prediction_time = time.time()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Arrêt Prediction Engine.")