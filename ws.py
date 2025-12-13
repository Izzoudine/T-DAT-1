import asyncio
import json
import os
import websockets
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

# Charge les variables d'environnement (si présentes)
load_dotenv()

# --------------------
# CONFIGURATION
# --------------------
# On prend l'IP du .env ou localhost par défaut
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BROKER', 'localhost:29092')
WS_PORT = 8000

# ON ÉCOUTE UNIQUEMENT LE TOPIC ANALYTICS
TARGET_TOPIC = 'analytics-updates'

clients = set()

async def register(ws):
    clients.add(ws)
    print(f"➕ Client Heatmap connecté ({len(clients)})")
    try:
        await ws.wait_closed()
    finally:
        clients.remove(ws)
        print(f"➖ Client parti ({len(clients)})")

async def broadcast(message):
    if clients:
        await asyncio.gather(*[client.send(message) for client in clients], return_exceptions=True)

async def consume_kafka():
    consumer = AIOKafkaConsumer(
        TARGET_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        
        # --- 🛑 CONFIGURATION HEATMAP ---
        auto_offset_reset='latest',  # On veut le direct (pas le passé)
        group_id='ws-heatmap-only',  # Nouveau groupe ID
        # --------------------------------
        
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"⏳ Connexion Kafka sur {TARGET_TOPIC}...")
    await consumer.start()
    print("✅ Kafka connecté ! (Mode: HEATMAP ONLY)")

    try:
        async for msg in consumer:
            data = msg.value
            
            # --- 🛑 FILTRAGE STRICT ---
            # On ne laisse passer QUE la Heatmap (on ignore les Whales ici)
            if data.get('type') == 'HEATMAP_UPDATE':
                
                # On prépare un JSON propre pour le Frontend
                payload = json.dumps({
                    "type": "HEATMAP_UPDATE",
                    "data": data['data'] # Le tableau des carrés
                })
                
                # Envoi au Frontend
                await broadcast(payload)
                # print("🔥 Heatmap envoyée aux clients")

    except Exception as e:
        print(f"❌ Erreur Kafka: {e}")
    finally:
        await consumer.stop()

async def main():
    async with websockets.serve(register, "0.0.0.0", WS_PORT):
        print(f"🚀 Serveur WebSocket Heatmap prêt sur ws://0.0.0.0:{WS_PORT}")
        await consume_kafka()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Arrêt.")