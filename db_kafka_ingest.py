import json
import psycopg2
import psycopg2.extras # Indispensable pour l'insertion rapide
import time
import os
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

# --- CHARGEMENT DES SECRETS (.env) ---
# Cela empêche l'erreur GitGuardian
load_dotenv()

# --- CONFIGURATION BDD ---
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# --- CONFIGURATION KAFKA ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICS = ['processed-article', 'price-topic']
GROUP_ID = 'db-ingest-optimized' 

# --- CONFIGURATION BUFFER ---
FLUSH_INTERVAL = 60 # Envoyer à la BDD toutes les 60 secondes
last_flush_time = time.time()
price_buffer = {} # Dictionnaire pour stocker les derniers prix

# --- CONNEXION BDD ---
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("✅ Connecté à TimescaleDB")
except Exception as e:
    print(f"❌ Erreur connexion BDD: {e}")
    exit()

# --- FONCTIONS ---

def flush_price_buffer():
    """Vide le buffer et envoie tout à la BDD en une seule fois"""
    global price_buffer, last_flush_time
    
    if not price_buffer:
        return # Rien à envoyer

    print(f"⏱️ Flush BDD: Envoi de {len(price_buffer)} paires cryptos...")
    
    # On prépare la liste des tuples pour SQL
    values_list = []
    for pair, data in price_buffer.items():
        ts = data.get('timestamp')
        # Gestion sécurité : si timestamp est float ou int, on convertit, sinon on laisse
        dt_object = datetime.fromtimestamp(ts) if isinstance(ts, (int, float)) else ts
        
        values_list.append((
            dt_object,
            pair,
            data.get('last'),
            data.get('bid'),
            data.get('ask'),
            data.get('volume_24h')
        ))

    # Requete SQL optimisée (Batch Insert)
    query = """
        INSERT INTO crypto_prices (datetime, pair, price, bid, ask, volume)
        VALUES %s
        ON CONFLICT (datetime, pair) DO NOTHING;
    """
    
    try:
        psycopg2.extras.execute_values(cursor, query, values_list)
        print("✅ Batch Price inséré avec succès.")
        
        # Reset du buffer et du timer
        price_buffer = {} 
        last_flush_time = time.time()
        
    except Exception as e:
        print(f"⚠️ Erreur Batch Insert: {e}")

def insert_article(data):
    """Les articles sont rares, on les insère immédiatement"""
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
        if cursor.rowcount > 0:
            print(f"📥 Article saved: {data.get('title')[:30]}...")
    except Exception as e:
        print(f"⚠️ Erreur Article: {e}")

# --- MAIN LOOP ---
def main():
    global last_flush_time
    
    print(f"🎧 Consumer démarré sur {KAFKA_BROKER}. Buffer: {FLUSH_INTERVAL}s.")
    
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest', # On veut le temps réel
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    while True:
        # 1. Vérification du Timer (Est-ce qu'on doit envoyer à la DB ?)
        if time.time() - last_flush_time >= FLUSH_INTERVAL:
            flush_price_buffer()

        # 2. Lecture des messages Kafka (Timeout court pour ne pas bloquer)
        msg_pack = consumer.poll(timeout_ms=1000) 

        for partition, messages in msg_pack.items():
            for message in messages:
                topic = message.topic
                data = message.value

                # CAS A : PRIX (Mise en mémoire tampon)
                if topic == 'price-topic':
                    pair = data.get('pair')
                    # On garde uniquement la dernière valeur reçue pour cette paire
                    price_buffer[pair] = data 
                
                # CAS B : ARTICLES (Insertion immédiate)
                elif topic == 'processed-article':
                    insert_article(data)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du script.")
        conn.close()