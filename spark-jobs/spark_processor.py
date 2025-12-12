from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, struct, to_json
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, FloatType, MapType
import re

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

NARRATIVE_KEYWORDS = {
    "SECURITY": [
        "hack", "hacked", "exploit", "stolen", "breach", "attacker", 
        "vulnerability", "phishing", "scam", "rug pull", "drain"
    ],
    "REGULATION": [
        "sec", "regulation", "regulators", "lawsuit", "sued", "ban", 
        "illegal", "compliant", "tax", "gensler", "government", "court","doj", "guilty", "pleads", "convicted", "sentence", "jail", "prison", "fine", "penalty"
    ],
    "LISTING_EXCHANGE": [
        "listing", "listed", "delist", "exchange", "binance", "coinbase", 
        "kraken", "withdrawals suspended", "liquidity"
    ],
    "MACRO_ECONOMY": [
        "inflation", "fed", "fomc", "interest rate", "cpi", "powell", 
        "recession", "economy", "dollar", "stocks", "nasdaq"
    ],
    "TECH_UPGRADE": [
        "mainnet", "testnet", "upgrade", "fork", "hard fork", "launch", 
        "release", "beta", "roadmap", "partnership", "integrate"
    ],
    "WHALE_MARKET": [
        "whale", "transfer", "wallet", "accumulation", "dump", 
        "all-time high", "ath", "resistance", "support", "bull run"
    ]
}

MASTER_CRYPTO_MAP = {
    # --- LES GÉANTS (L1 & Stores of Value) ---
    "BTC": ["bitcoin", "btc", "satoshi", "xbt"],
    "ETH": ["ethereum", "ether", "eth", "vitalik"], # 'Vitalik' souvent synonyme d'ETH dans les titres
    "SOL": ["solana", "sol"],
    "BNB": ["binance coin", "bnb", "bsc", "binance smart chain"],
    "XRP": ["ripple", "xrp", "xrp ledger"],
    "ADA": ["cardano", "ada"],
    "AVAX": ["avalanche", "avax"],
    "TRX": ["tron", "trx", "justin sun"],
    "DOT": ["polkadot", "dot"], # Attention: 'dot' est un mot courant, voir section regex
    "MATIC": ["polygon", "matic", "pol"], # POL est le nouveau ticker, mais mot risqué
    "TON": ["toncoin", "ton", "the open network"],
    "KAS": ["kaspa", "kas"],

    # --- STABLECOINS (Important pour détecter les flux) ---
    "USDT": ["tether", "usdt"],
    "USDC": ["usd coin", "usdc", "circle"],
    "DAI": ["dai", "makerdao"],
    "FDUSD": ["first digital usd", "fdusd"],

    # --- DEFI & INFRA ---
    "LINK": ["chainlink", "link"], # 'link' est risqué
    "UNI": ["uniswap", "uni"],
    "AAVE": ["aave"],
    "LDO": ["lido", "ldo"],
    "ARB": ["arbitrum", "arb"],
    "OP": ["optimism", "op"],
    "TIA": ["celestia", "tia"],
    "INJ": ["injective", "inj"],
    "RUNE": ["thorchain", "rune"],
    
    # --- AI & BIG DATA (Tendance actuelle) ---
    "FET": ["fetch.ai", "fetch", "fet", "asi"],
    "RNDR": ["render", "rndr", "render token"],
    "NEAR": ["near protocol", "near"], # 'near' est très risqué (préposition)
    "WLD": ["worldcoin", "wld", "sam altman"],
    "TAO": ["bittensor", "tao"],

    # --- MEMECOINS (Le plus de volatilité) ---
    "DOGE": ["dogecoin", "doge"],
    "SHIB": ["shiba inu", "shib", "shibarium"],
    "PEPE": ["pepe", "pepecoin"],
    "WIF": ["dogwifhat", "wif"],
    "BONK": ["bonk"],
    "FLOKI": ["floki", "floki inu"],
    "BRETT": ["brett"],
    "POPCAT": ["popcat"],
}

RISKY_KEYWORDS = {
    "one", "link", "dot", "near", "uni", "bond", "gas", "pol", "rose", "trump", "cat", "baby"
}

def analyze_sentiment_score(text):
    if not text: return 0.0
    return float(analyzer.polarity_scores(text)['compound'])

def analyze_sentiment_label(score):
    if score >= 0.05: return "positive"
    if score <= -0.05: return "negative"
    return "neutral"

def extract_cryptos(text, description):
    content = (str(text) + " " + str(description)).lower()
    found = set()
    for ticker, syns in MASTER_CRYPTO_MAP.items():
        for syn in syns:
            pattern = r'\b' + re.escape(syn) + r'\b'
            if re.search(pattern, content):
                # Check risky keywords
                if syn in RISKY_KEYWORDS:
                    if not re.search(r'\b' + re.escape(syn.upper()) + r'\b', content.upper()):
                        continue
                found.add(ticker)
                break 
    return list(found)

def determine_narrative(description):
    text = str(description).lower()
    scores = {cat: 0 for cat in NARRATIVE_KEYWORDS}
    for cat, kws in NARRATIVE_KEYWORDS.items():
        for word in kws:
            if word in text: scores[cat] += 1
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "GENERAL_NEWS"

# Enregistrement des UDFs
udf_sentiment_score = udf(analyze_sentiment_score, FloatType())
udf_sentiment_label = udf(analyze_sentiment_label, StringType())
udf_cryptos = udf(extract_cryptos, ArrayType(StringType()))
udf_narrative = udf(determine_narrative, StringType())

# --- MAIN SPARK JOB ---

spark = SparkSession.builder \
    .appName("CryptoNewsProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 1. Lecture Stream depuis Kafka (Topic brut)
# Utilisation du port INTERNE (29092) car Spark et Kafka sont dans le même réseau Docker
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "article-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Parsing du JSON
schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("link", StringType()),
    StructField("website", StringType()),
    StructField("time", StringType())
])

df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 3. Application de l'IA (Enrichissement)
df_enriched = df_parsed \
    .withColumn("cryptos", udf_cryptos(col("title"), col("description"))) \
    .withColumn("narrative", udf_narrative(col("description"))) \
    .withColumn("sentiment_score", udf_sentiment_score(col("description"))) \
    .withColumn("sentiment_label", udf_sentiment_label(col("sentiment_score")))

# 4. Formatage Final pour le Topic de Sortie
# On structure le JSON final exactement comme demandé
df_final = df_enriched.select(
    struct(
        col("id"),
        col("title"),
        col("description"),
        col("link"),
        col("website"),
        col("time"),
        col("cryptos"),
        col("narrative"),
        struct(
            col("sentiment_score").alias("score"),
            col("sentiment_label").alias("label")
        ).alias("sentiment")
    ).alias("value") # Kafka attend une colonne "value"
)

# 5. Conversion en JSON String et écriture dans Kafka
query = df_final \
    .select(to_json(col("value")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "processed-article") \
    .option("checkpointLocation", "/opt/bitnami/spark/checkpoints") \
    .start()

query.awaitTermination()