#!/bin/bash
# chmod +x startup.sh
# ./startup.sh
echo "🚀 Starting Kafka + Python data pipeline..."
sudo pkill -f python3
# ----------------------------
# 1) Lancer Docker (Kafka + Zookeeper)
# ----------------------------

echo "🐳 Starting Docker containers..."
docker compose up -d


# ----------------------------
# 2) CONFIGURATION MAGIC HOST (Nouveau !)
# ----------------------------
# On map 'kafka' vers '127.0.0.1' pour que le script Python sur la VM
# puisse parler au container via le nom qu'il annonce.
echo "🔧 Configuring /etc/hosts for Kafka..."

if grep -q "127.0.0.1 kafka" /etc/hosts; then
    echo "✅ Host entry 'kafka' already exists."
else
    echo "➕ Adding '127.0.0.1 kafka' to /etc/hosts..."
    # sudo est nécessaire ici. Le script te demandera ton mot de passe si besoin.
    echo "127.0.0.1 kafka" | sudo tee -a /etc/hosts > /dev/null
fi

# ----------------------------
# 3) Créer environnement Python
# ----------------------------
if [ ! -d "venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv venv
fi

echo "📦 Activating venv and installing dependencies..."
source venv/bin/activate
python3 -m pip install -r requirements.txt

# ----------------------------
# 4) Supprimer les anciens topics Kafka
# ----------------------------

docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic price-topic
docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic trade-topic
docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic alert-topic
docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic article-topic

# ----------------------------
# 5) Creer les topics Kafka
# ----------------------------

echo "📌 Creating Kafka topics..."
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic price-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic trade-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic alert-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic article-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic processed-article --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list

# ----------------------------
# 6) Préparer et Lancer SPARK
# ----------------------------
echo "⚡ Preparing Spark environment..."

# 1. Installation sur le Master (pour que le Driver puisse charger le script)
echo "Installing Vader on Master..."
docker exec spark-master pip install vaderSentiment

# 2. Installation sur le Worker (pour que les tâches puissent s'exécuter)
echo "Installing Vader on Worker..."
docker exec spark-worker pip install vaderSentiment

echo "🔥 Submitting Spark Job..."
docker exec -d spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark_processor.py

# ----------------------------
# 7) Lancer les scripts en background
# ----------------------------
echo "📡 Starting price-topic.py in background..."
nohup python3 price-topic.py > price.log 2>&1 &

echo "📰 Starting article-topic.py in background..."
nohup python3 article-topic.py > article.log 2>&1 &

# ----------------------------
# 8) Afficher les derniers logs
# ----------------------------

sleep 2
echo "📊 Last 10 lines of price.log:"
tail -n 10 price.log

echo "📰 Last 10 lines of article.log:"
tail -n 10 article.log

echo "Making the topics available"
nohup python3 -u ws.py > ws.log 2>&1 &

echo "✅ System started successfully!"



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
