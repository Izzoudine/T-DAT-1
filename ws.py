import asyncio
import json
import websockets
from aiokafka import AIOKafkaConsumer

# --------------------
# CONFIGURATION
# --------------------
KAFKA_BOOTSTRAP = 'localhost:29092' 
KAFKA_TOPICS = ['price-topic', 'trade-topic', 'alert-topic', 'article-topic']
WS_PORT = 8000

clients = set()

async def register(ws):
    clients.add(ws)
    print(f"➕ Client connecté ({len(clients)})")
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
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        
        # --- 🛑 MODIFICATION IMPORTANTE ICI ---
        auto_offset_reset='earliest',  # On lit l'historique au démarrage !
        group_id='ws-server-v2',       # Nom de groupe unique pour suivre la lecture
        # --------------------------------------
        
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"⏳ Connexion Kafka...")
    await consumer.start()
    print("✅ Kafka connecté ! (Mode: EARLIEST)")

    try:
        async for msg in consumer:
            data = msg.value
            
            # Debug spécifique pour voir si les articles passent
            if msg.topic == 'article-topic':
                print(f"📰 ARTICLE DÉTECTÉ : {str(data.get('title', 'No Title'))}")

            payload = json.dumps({
                "topic": msg.topic,
                "data": data
            })
            
            await broadcast(payload)
    except Exception as e:
        print(f"❌ Erreur Kafka: {e}")
    finally:
        await consumer.stop()

async def main():
    async with websockets.serve(register, "0.0.0.0", WS_PORT):
        print(f"🚀 WebSocket ws://0.0.0.0:{WS_PORT}")
        await consume_kafka()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Arrêt.")