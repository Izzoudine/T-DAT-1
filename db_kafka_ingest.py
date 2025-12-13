import json
import psycopg2
import psycopg2.extras
import time
import os
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')

# Liste des topics écoutés
TOPICS = ['processed-article', 'price-topic', 'narrative-events', 'analytics-updates']
GROUP_ID = 'db-ingest-final' 

# --- BUFFER PRIX ---
FLUSH_INTERVAL = 60 
last_flush_time = time.time()
price_stats = {} 

# --- CONNEXION DB ---
try:
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    conn.autocommit = True
    cursor = conn.cursor()
    print("✅ Connecté à TimescaleDB")
except Exception as e:
    print(f"❌ Erreur BDD: {e}")
    exit()

# --- FONCTIONS D'INSERTION ---

def insert_narrative_event(data):
    """Sauvegarde le résumé généré par Gemini"""
    try:
        query = """
            INSERT INTO narrative_events (event_id, datetime, main_crypto, narrative_category, headline, sentiment_score, impact_level, source_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """
        ts = data.get('timestamp')
        dt = datetime.fromtimestamp(ts)

        cursor.execute(query, (
            data.get('event_id'),
            dt,
            data.get('main_crypto'),
            data.get('narrative_category'),
            data.get('headline'),
            data.get('sentiment_score'),
            data.get('impact_level'),
            data.get('source_count')
        ))
        print(f"🧠 AI Event Saved: {data.get('headline')}")
    except Exception as e:
        print(f"⚠️ Erreur Event: {e}")

def insert_whale_alert(data):
    """Sauvegarde les alertes Whales"""
    # On ne sauvegarde QUE les alertes WHALE, pas les Heatmaps (trop fréquentes)
    if data.get('type') != 'WHALE_ALERT':
        return

    try:
        query = """
            INSERT INTO whale_alerts (datetime, pair, side, price, amount_usd, trigger_threshold)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        # Le timestamp vient du message analytics
        ts = data.get('timestamp')
        dt = datetime.fromtimestamp(ts)
        
        cursor.execute(query, (
            dt,
            data.get('pair'),
            data.get('side'),      # BUY ou SELL
            data.get('price'),
            data.get('amount_usd'),
            data.get('threshold_used', 0) # On récupère le seuil utilisé
        ))
        print(f"🐋 Whale Saved: {data.get('side')} {data.get('pair')} ${data.get('amount_usd'):,.0f}")
    except Exception as e:
        print(f"⚠️ Err Whale: {e}")
def flush_price_buffer():
    """Agrège les prix minute par minute et inclut le pct_change"""
    global price_stats, last_flush_time
    
    if not price_stats:
        return

    # Log pour le debug
    # print(f"⏱️ Flush: Envoi de {len(price_stats)} paires...")

    values_list = []
    for pair, stats in price_stats.items():
        # 1. Moyenne du prix sur la minute
        avg_price = stats['sum_price'] / stats['count']
        
        # 2. Récupération des dernières infos
        last_data = stats['last_data']
        ts = last_data.get('timestamp')
        dt_object = datetime.fromtimestamp(ts) if isinstance(ts, (int, float)) else ts
        
        # 3. Sécurité Anti-NULL pour Bid et Ask (on met 0.0 si absent)
        bid = last_data.get('bid')
        if bid is None: bid = 0.0
        
        ask = last_data.get('ask')
        if ask is None: ask = 0.0

        # 4. Gestion du pct_change (Nouveau)
        pct = last_data.get('pct_change')
        if pct is None: pct = 0.0
        
        values_list.append((
            dt_object, 
            pair, 
            avg_price, 
            bid, 
            ask, 
            last_data.get('volume_24h'), 
            pct
        ))

    # Mise à jour de la requête SQL avec pct_change
    query = """
        INSERT INTO crypto_prices (datetime, pair, price, bid, ask, volume, pct_change)
        VALUES %s 
        ON CONFLICT (datetime, pair) DO NOTHING;
    """
    
    try:
        psycopg2.extras.execute_values(cursor, query, values_list)
        print("✅ Moyennes Prix (avec pct_change) insérées.")
        price_stats = {}
        last_flush_time = time.time()
    except Exception as e:
        print(f"⚠️ Erreur Batch: {e}")

def insert_article(data):
    """Sauvegarde les articles bruts traités par Spark"""
    try:
        sentiment = data.get('sentiment', {})
        query = """
            INSERT INTO articles (id, title, description, link, website, datetime, cryptos, narrative, sentiment_score, sentiment_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """
        cursor.execute(query, (
            data.get('id'), data.get('title'), data.get('description'), data.get('link'),
            data.get('website'), data.get('time'), data.get('cryptos', []),
            data.get('narrative'), sentiment.get('score', 0.0), sentiment.get('label', 'neutral')
        ))
    except Exception as e: 
        print(f"⚠️ Erreur Article: {e}")

# --- MAIN ---
def main():
    global last_flush_time
    print(f"🎧 DB Ingest écoute : {TOPICS}")
    
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    while True:
        # Vérification du timer pour vider le buffer prix (chaque 60s)
        if time.time() - last_flush_time >= FLUSH_INTERVAL:
            flush_price_buffer()

        msg_pack = consumer.poll(timeout_ms=1000) 

        for partition, messages in msg_pack.items():
            for message in messages:
                topic = message.topic
                data = message.value

                # 1. C'est un PRIX (Moyenne)
                if topic == 'price-topic':
                    pair = data.get('pair')
                    price = float(data.get('last', 0))
                    
                    if pair not in price_stats:
                        price_stats[pair] = {"sum_price": 0.0, "count": 0, "last_data": data}
                    
                    price_stats[pair]["sum_price"] += price
                    price_stats[pair]["count"] += 1
                    # On garde le dernier packet car il contient le Bid/Ask/Pct le plus récent
                    price_stats[pair]["last_data"] = data
                
                # 2. C'est un ARTICLE BRUT
                elif topic == 'processed-article':
                    insert_article(data)
                
                # 3. C'est un RÉSUMÉ IA
                elif topic == 'narrative-events':
                    insert_narrative_event(data)

                elif topic == 'analytics-updates':
                    insert_whale_alert(data)    

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        conn.close()