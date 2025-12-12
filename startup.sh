#!/bin/bash
# chmod +x startup.sh
# ./startup.sh
echo "🚀 Starting Kafka + Spark (Apache) + Python data pipeline..."
sudo pkill -f python3

# ----------------------------
# 1) Lancer Docker
# ----------------------------
echo "🐳 Starting Docker containers..."
docker compose up -d

echo "⏳ Waiting 15s for services to stabilize..."
sleep 15

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
# 3) Python Venv
# ----------------------------
if [ ! -d "venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv venv
fi
source venv/bin/activate
python3 -m pip install -r requirements.txt

# ----------------------------
# 4) Kafka Topics
# ----------------------------
echo "📌 Creating Kafka topics..."
# On supprime d'abord pour être propre
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic price-topic --if-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic trade-topic --if-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic alert-topic --if-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic article-topic --if-exists
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic processed-article --if-exists

# On crée
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic price-topic --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic trade-topic --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic alert-topic --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic article-topic --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic processed-article --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list

# ----------------------------
# 5) Lancer SPARK (IMAGE APACHE)
# ----------------------------
echo "⚡ Preparing Spark environment..."

# Installation de Vader sur Master et Worker
echo "Installing libs on Master..."
docker exec spark-master pip install vaderSentiment
echo "Installing libs on Worker..."
docker exec spark-worker pip install vaderSentiment

echo "🔥 Submitting Spark Job..."
# NOTE BIEN LES NOUVEAUX CHEMINS (/opt/spark/...)
docker exec -d spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/spark_processor.py

# ----------------------------
# 6) Lancer Scripts Python
# ----------------------------
echo "📡 Starting price-topic.py..."
nohup python3 price-topic.py > price.log 2>&1 &

echo "📰 Starting article-topic.py..."
nohup python3 article-topic.py > article.log 2>&1 &

echo "🌐 Starting ws.py..."
nohup python3 -u ws.py > ws.log 2>&1 &

sleep 2
echo "✅ System started!"


# read the messages

# docker exec -it kafka kafka-console-consumer \
    #--bootstrap-server kafka:29092 \
    #--topic price-topic \
    #--from-beginning

# ps aux | grep ws.py
# lister les process de ws lancer en background
# kill -9 ID en supprimer 

# process sur le port 9092
# sudo lsof -i :9092
