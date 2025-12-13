import json
import time
import os
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_SERVER = os.getenv('KAFKA_BROKER')
INPUT_TOPIC = 'trade-topic'          # On écoute les trades bruts
OUTPUT_TOPIC = 'analytics-updates'   # On envoie les analyses

# Fenêtre de temps pour la Heatmap (60 secondes glissantes)
HEATMAP_WINDOW_SECONDS = 60

# --- SEUILS WHALES (En Dollars) ---
# Si un trade dépasse ce montant, on lance une alerte
WHALE_THRESHOLDS = {
    "XBT/USD": 100000,  # 100k$ pour Bitcoin
    "ETH/USD": 50000,   # 50k$ pour Ethereum
    "SOL/USD": 20000,   # 20k$ pour Solana
    "default": 10000    # 10k$ pour les autres
}

# --- STATE (Mémoire Vive) ---
# history['BTC/USD'] = deque([(timestamp, volume_usd, side), ...])
trade_history = defaultdict(deque)

# --- KAFKA SETUP ---
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🚀 Analytics Engine démarré (Topic: {INPUT_TOPIC} -> {OUTPUT_TOPIC})")

last_heatmap_emit = time.time()

def process_whale_alert(trade):
    """Détecte les gros trades uniques"""
    try:
        pair = trade.get('pair')
        price = float(trade.get('price', 0))
        volume = float(trade.get('volume', 0))
        side = trade.get('side')
        timestamp = trade.get('timestamp')

        usd_value = price * volume
        
        # Récupération du seuil spécifique
        threshold = WHALE_THRESHOLDS.get(pair, WHALE_THRESHOLDS["default"])
        
        if usd_value >= threshold:
            alert = {
                "type": "WHALE_ALERT",
                "pair": pair,
                "side": "BUY" if side == 'b' else "SELL",
                "price": price,
                "amount_usd": round(usd_value, 2),
                "threshold_used": threshold,
                "timestamp": timestamp
            }
            producer.send(OUTPUT_TOPIC, alert)
            
            # Petit log visuel sympa
            icon = "🟢" if side == 'b' else "🔴"
            print(f"{icon} WHALE: {pair} ${alert['amount_usd']:,.0f} (> ${threshold})")
            
    except Exception as e:
        print(f"⚠️ Erreur Whale Check: {e}")

def update_heatmap_state(trade):
    """Ajoute le trade à l'historique pour le calcul du Delta"""
    try:
        pair = trade.get('pair')
        price = float(trade.get('price', 0))
        volume = float(trade.get('volume', 0))
        side = trade.get('side')
        
        usd_value = price * volume
        now = time.time()
        
        # Ajouter au buffer mémoire
        trade_history[pair].append({
            "ts": now,
            "val": usd_value,
            "side": side
        })
    except Exception:
        pass

def emit_heatmap_update():
    """Calcule le volume et le delta sur la dernière minute"""
    now = time.time()
    heatmap_data = []

    for pair, history in trade_history.items():
        # 1. Nettoyer les vieux trades (> 60s)
        while history and history[0]['ts'] < (now - HEATMAP_WINDOW_SECONDS):
            history.popleft()
        
        if not history:
            continue
            
        # 2. Calculer les métriques (Achat vs Vente)
        buy_vol = sum(t['val'] for t in history if t['side'] == 'b')
        sell_vol = sum(t['val'] for t in history if t['side'] == 's')
        
        total_vol = buy_vol + sell_vol
        net_delta = buy_vol - sell_vol # Positif = Achat dominant
        
        # Ratio Imbalance (-1 à +1) pour la couleur
        imbalance_ratio = net_delta / total_vol if total_vol > 0 else 0
        
        heatmap_data.append({
            "pair": pair,
            "vol_1m": round(total_vol, 0),
            "delta_1m": round(net_delta, 0),
            "imb": round(imbalance_ratio, 2)
        })
    
    if heatmap_data:
        payload = {
            "type": "HEATMAP_UPDATE",
            "data": heatmap_data
        }
        producer.send(OUTPUT_TOPIC, payload)

# --- MAIN LOOP ---
try:
    for message in consumer:
        trade = message.value
        
        # 1. Analyse Whale (Immédiat)
        process_whale_alert(trade)
        
        # 2. Mise à jour Buffer Heatmap
        update_heatmap_state(trade)
        
        # 3. Émission Heatmap (Toutes les 2 secondes pour ne pas spammer)
        if time.time() - last_heatmap_emit >= 2:
            emit_heatmap_update()
            last_heatmap_emit = time.time()

except KeyboardInterrupt:
    print("Arrêt Analytics.")
    producer.close()