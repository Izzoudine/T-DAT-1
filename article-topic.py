import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import schedule
from kafka import KafkaProducer
import json
import hashlib

# Configuration Kafka
# On utilise l'IP publique car ce script tourne probablement hors du conteneur (ou sur ta machine locale)
KAFKA_BOOTSTRAP_SERVERS = '20.199.136.163:9092'
TOPIC_NAME = 'article-topic'

producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1,
        retries=3
    )
    print(f"✅ Connecté à Kafka sur {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"❌ Erreur connexion Kafka: {e}")

def clean_html(raw_html):
    if not raw_html: return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def scrapper(url, website):
    print(f"🚀 Scraping {website}...")
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')

        if not items: return

        for item in items:
            title_tag = item.find("title")
            link_tag = item.find("guid") or item.find("link")
            desc_tag = item.find("description")

            if not title_tag or not link_tag: continue

            extracted_title = title_tag.get_text(strip=True)
            extracted_link = link_tag.get_text(strip=True)
            raw_desc = desc_tag.get_text(strip=True) if desc_tag else ""
            clean_desc = clean_html(raw_desc)

            # Filtre basique
            if "/videos/" in extracted_link or len(clean_desc) < 20: continue

            # ID Unique
            article_id = hashlib.md5(extracted_link.encode('utf-8')).hexdigest()

            # Structure demandée pour la Partie 1
            article_raw = {
                "id": article_id,
                "title": extracted_title,
                "description": clean_desc,
                "link": extracted_link,
                "website": website,
                "time": datetime.utcnow().isoformat()
            }

            if producer:
                producer.send(TOPIC_NAME, article_raw)
                # print(f"Sent: {extracted_title[:30]}...")

        if producer:
            producer.flush()
            print(f"✅ {website}: Lot envoyé à Kafka.")

    except Exception as e:
        print(f"❌ Erreur {website}: {e}")

def job():
    scrapper("https://cointelegraph.com/rss", "cointelegraph.com")
    scrapper("https://coindesk.com/arc/outboundfeeds/rss", "coindesk.com")
    scrapper("https://decrypt.co/feed", "decrypt.co")

if __name__ == "__main__":
    job()
    schedule.every(5).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(10)