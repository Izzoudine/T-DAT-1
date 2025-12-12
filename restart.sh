#!/bin/bash
# chmod +x restart.sh
# ./restart.sh

# 1. On force l'arrêt des conteneurs (peu importe le dossier)
echo "🛑 Arrêt forcé des conteneurs..."
docker rm -f zookeeper kafka spark-master spark-worker 2>/dev/null

# 2. On supprime les volumes orphelins (Correction Cluster ID Mismatch)
echo "🧹 Nettoyage des volumes..."
docker volume prune -f
docker volume rm t-dat-1_kafka-data t-dat-1_zookeeper-data 2>/dev/null

# 3. On retourne à la racine et on supprime le dossier
cd ~
echo "🗑️ Suppression de l'ancien dossier..."
sudo rm -rf T-DAT-1

# 4. Clonage
echo "⬇️ Clonage du repository..."
git clone https://github.com/Izzoudine/T-DAT-1.git
cd T-DAT-1

# 5. CRÉATION DU FICHIER .ENV
# ⚠️ REMPLACE CI-DESSOUS PAR TON VRAI NOUVEAU MOT DE PASSE TIMESCALE
echo "🔑 Création du fichier .env..."
cat <<EOF > .env
DB_HOST=c9obawmetw.l28b4d0kwr.tsdb.cloud.timescale.com
DB_PORT=34828
DB_NAME=tsdb
DB_USER=tsdbadmin
DB_PASSWORD=n6ev6kdbxycgn40b
KAFKA_BROKER=20.199.136.163:9092
EOF

# 6. Lancement
echo "🚀 Lancement du startup..."
chmod +x startup.sh
./startup.sh