import json
import time
import os
import itertools
import psycopg2
import psycopg2.extras
from collections import defaultdict
from kafka import KafkaProducer
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
from dotenv import load_dotenv

# --- 1. CHARGEMENT SECRETS ---
load_dotenv()

# Clés API Gemini (séparées par des virgules dans le .env)
keys_str = os.getenv("GEMINI_API_KEYS")
if not keys_str:
    print("❌ Erreur: GEMINI_API_KEYS manquant dans .env")
    exit()

API_KEYS = keys_str.split(',')
key_iterator = itertools.cycle(API_KEYS)

def switch_api_key():
    """Rotation automatique des clés API"""
    try:
        new_key = next(key_iterator)
        genai.configure(api_key=new_key)
        print(f"🔑 Gemini Key Active: ...{new_key[-4:]}")
    except Exception as e:
        print(f"⚠️ Erreur Key: {e}")

# Initialisation
switch_api_key()
model = genai.GenerativeModel('gemini-2.5-flash')

# --- 2. CONFIG DB & KAFKA ---
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

KAFKA_SERVER = os.getenv('KAFKA_BROKER')
OUTPUT_TOPIC = 'narrative-events' # C'est ici qu'on remplit le topic !

# Paramètres
POLL_INTERVAL = 60  # Vérifie les news toutes les minutes
MIN_ARTICLES = 2    # Il faut 2 articles pour créer un événement (sauf urgence)

# Producer Kafka (Pour envoyer le résultat)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def get_gemini_summary(articles, crypto, category):
    """Appelle l'IA pour résumer"""
    titles = [f"- {a['title']}" for a in articles]
    titles_text = "\n".join(titles)
    
    prompt = f"""
    Role: Senior Crypto Analyst.
    Task: Analyze these headlines about {crypto} ({category}).
    Output: Write ONE single, powerful sentence (max 15 words) explaining exactly what is driving the market. No intro.
    Headlines:
    {titles_text}
    """
    
    for _ in range(3): # 3 Essais en cas d'erreur
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except ResourceExhausted:
            print("⚠️ Quota dépassé, changement de clé...")
            switch_api_key()
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Gemini Error: {e}")
            return f"Significant market movement detected on {crypto}."
            
    return f"Multiple reports concerning {crypto}."

def process_batch():
    """Le coeur du réacteur"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # A. On cherche les articles NON traités
        # On trie par date pour traiter les plus vieux d'abord
        cursor.execute("""
            SELECT * FROM articles 
            WHERE ai_processed = FALSE 
            ORDER BY datetime ASC 
            LIMIT 50
        """)
        rows = cursor.fetchall()
        
        if not rows:
            # Rien à faire, on dort
            return

        print(f"🔄 Traitement IA de {len(rows)} nouveaux articles...")
        
        # B. On les regroupe (Clustering)
        # Clé du groupe = (Crypto, Narratif)
        clusters = defaultdict(list)
        processed_ids = []

        for row in rows:
            processed_ids.append(row['id'])
            
            narrative = row['narrative']
            cryptos = row['cryptos'] if row['cryptos'] else [] # Peut être None ou vide
            
            if not cryptos:
                # Si pas de crypto spécifique, c'est du MARKET global
                clusters[('MARKET', narrative)].append(row)
            else:
                # Si l'article parle de BTC et ETH, il va dans les 2 groupes
                for coin in cryptos:
                    clusters[(coin, narrative)].append(row)

        # C. Analyse IA pour chaque groupe
        events_generated = 0
        
        for key, articles in clusters.items():
            crypto, narrative = key
            
            # Règle : Sécurité/Régulation = Urgent (1 article suffit)
            # Sinon il faut au moins MIN_ARTICLES
            is_urgent = narrative in ['SECURITY', 'REGULATION']
            
            if len(articles) >= MIN_ARTICLES or (is_urgent and len(articles) >= 1):
                
                # 1. Génération du résumé
                summary = get_gemini_summary(articles, crypto, narrative)
                
                # 2. Calcul sentiment moyen
                avg_sent = sum(a['sentiment_score'] for a in articles) / len(articles)
                
                # 3. Création de l'Event JSON
                event = {
                    "event_id": f"evt_{int(time.time())}_{crypto}_{narrative[:3]}",
                    "timestamp": time.time(),
                    "time_str": time.strftime("%H:%M:%S"),
                    "main_crypto": crypto,
                    "narrative_category": narrative,
                    "headline": summary,
                    "sentiment_score": round(avg_sent, 3),
                    "impact_level": "HIGH" if abs(avg_sent) > 0.4 else "MEDIUM",
                    "source_count": len(articles)
                }
                
                # 4. Envoi dans Kafka (topic: narrative-events)
                producer.send(OUTPUT_TOPIC, event)
                print(f"🧠 EVENT GÉNÉRÉ: [{crypto}] {summary}")
                events_generated += 1

        # D. Mise à jour de la DB (On marque comme traités)
        if processed_ids:
            cursor.execute("UPDATE articles SET ai_processed = TRUE WHERE id = ANY(%s)", (processed_ids,))
            conn.commit()
            print(f"✅ {len(processed_ids)} articles marqués comme traités.")
            if events_generated > 0:
                print(f"🚀 {events_generated} événements envoyés à Kafka.")

    except Exception as e:
        print(f"❌ Erreur DB/IA: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

# --- MAIN LOOP ---
print("🧠 Narrative Engine (DB Mode) Started...")
print(f"🎯 Output Topic: {OUTPUT_TOPIC}")

try:
    while True:
        process_batch()
        # Pause intelligente : inutile de spammer la DB si pas de news
        time.sleep(POLL_INTERVAL) 
except KeyboardInterrupt:
    print("Arrêt.")
    producer.close()