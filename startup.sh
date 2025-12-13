#!/bin/bash
# chmod +x startup.sh
# ./startup.sh

echo "🧹 Cleaning up old containers..."
docker compose down --remove-orphans
docker rm -f zookeeper kafka spark-master spark-worker 2>/dev/null

echo "🚀 Starting Kafka + Spark (Apache) + Python data pipeline..."
sudo pkill -f python3

# ----------------------------
# 1) Lancer Docker
# ----------------------------
echo "🐳 Starting Docker containers..."
docker compose up -d

# IMPORTANT : 15s est trop court, on met 40s pour éviter le crash de Kafka
echo "⏳ Waiting 40s for services to stabilize..."
sleep 40

# ----------------------------
# 2) Config HOST
# ----------------------------
echo "🔧 Configuring /etc/hosts for Kafka..."
if grep -q "127.0.0.1 kafka" /etc/hosts; then
    echo "✅ Host entry 'kafka' already exists."
else
    echo "127.0.0.1 kafka" | sudo tee -a /etc/hosts > /dev/null
fi

# ----------------------------
# 3) Python Venv & Dépendances
# ----------------------------
if [ ! -d "venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv venv
fi
source venv/bin/activate

echo "📦 Installing dependencies..."
python3 -m pip install -r requirements.txt

# ----------------------------
# 4) Kafka Topics
# ----------------------------
echo "📌 Creating Kafka topics..."

# On ne supprime pas (delete) pour garder l'historique si on restart juste le script
# On crée s'ils n'existent pas
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic price-topic --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic trade-topic --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic alert-topic --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic article-topic --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic processed-article --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic narrative-events --partitions 1 --replication-factor 1 --if-not-exists


docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# ----------------------------
# 5) Lancer SPARK (IMAGE APACHE)
# ----------------------------
echo "⚡ Preparing Spark environment..."

echo "Installing libs on Master & Worker..."
docker exec spark-master pip install vaderSentiment
docker exec spark-worker pip install vaderSentiment

echo "🔥 Submitting Spark Job..."
docker exec -d spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/spark_processor.py

# ----------------------------
# 6) Lancer Scripts Python Locaux
# ----------------------------
echo "📡 Starting price-topic.py..."
nohup python3 -u price-topic.py > price.log 2>&1 &

echo "📰 Starting article-topic.py..."
nohup python3 -u article-topic.py > article.log 2>&1 &

echo "💾 Starting db_kafka_ingest.py..."
nohup python3 -u db_kafka_ingest.py > db.log 2>&1 &

echo "🧠 Starting narrative_engine_db.py..."
nohup python3 -u narrative_engine_db.py > narrative.log 2>&1 &

echo "🧠 Starting analytics_engine.py..."
nohup python3 -u analytics_engine.py > analytics.log 2>&1 &

echo "🧠 Starting ws.py..."
nohup python3 -u ws.py > ws.log 2>&1 &

sleep 2
echo "✅ System started successfully!"
echo "📊 Logs available: price.log, article.log, db.log, ws.log"