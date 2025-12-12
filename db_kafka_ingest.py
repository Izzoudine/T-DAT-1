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

# ON AJOUTE LE NOUVEAU TOPIC ICI
TOPICS = ['processed-article', 'price-topic', 'narrative-events']
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
        # Conversion du timestamp UNIX en objet datetime
        ts = data.get('timestamp')
        dt = datetime.fromtimestamp(ts)

        cursor.execute(query, (
            data.get('event_id'),
            dt,
            data.get('main_crypto'),
            data.get('narrative_category'), # Attention au nom de clé dans ton producer
            data.get('headline'),
            data.get('sentiment_score'), # Attention, parfois c'est 'sentiment' ou 'sentiment_score' selon ton script IA
            data.get('impact_level'),    # ou 'impact'
            data.get('source_count')
        ))
        print(f"🧠 AI Event Saved: {data.get('headline')}")
    except Exception as e:
        print(f"⚠️ Erreur Event: {e}")

def flush_price_buffer():
    # ... (Même code que précédemment pour la moyenne des prix) ...
    global price_stats, last_flush_time
    if not price_stats: return

    # ... Logique de calcul moyenne ...
    values_list = []
    for pair, stats in price_stats.items():
        avg_price = stats['sum_price'] / stats['count']
        last_data = stats['last_data']
        ts = last_data.get('timestamp')
        dt_object = datetime.fromtimestamp(ts) if isinstance(ts, (int, float)) else ts
        
        values_list.append((dt_object, pair, avg_price, last_data.get('bid'), last_data.get('ask'), last_data.get('volume_24h')))

    query = """
        INSERT INTO crypto_prices (datetime, pair, price, bid, ask, volume)
        VALUES %s ON CONFLICT (datetime, pair) DO NOTHING;
    """
    try:
        psycopg2.extras.execute_values(cursor, query, values_list)
        print("✅ Moyennes Prix insérées.")
        price_stats = {}
        last_flush_time = time.time()
    except Exception as e:
        print(f"⚠️ Erreur Batch: {e}")

def insert_article(data):
    # ... (Même code que précédemment pour les articles bruts) ...
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
    except Exception as e: print(f"⚠️ Erreur Article: {e}")

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
                    price_stats[pair]["last_data"] = data
                
                # 2. C'est un ARTICLE BRUT
                elif topic == 'processed-article':
                    insert_article(data)
                
                # 3. C'est un RÉSUMÉ IA (NOUVEAU)
                elif topic == 'narrative-events':
                    insert_narrative_event(data)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        conn.close()