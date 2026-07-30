import time
import json
import logging
import requests
from kafka import KafkaProducer

# Setup clean production-grade logging metrics
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("TelemetryProducer")

# Live Real-World Data Source: Free Crypto Stream API (CoinGecko public feed)
# Alternatively, can swap with OpenAQ climate metrics feeds seamlessly
API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_vol=true"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "live_market_telemetry"

def initialize_producer() -> KafkaProducer:
    """Establishes a highly resilient connection hook to the Kafka messaging broker network."""
    for attempt in range(5):
        try:
            # We configure a JSON serializer to automatically encode python dicts to byte-arrays
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1 # Guarantees the leader broker acknowledges the write securely
            )
            logger.info("Successfully bound network socket to Kafka Broker Link.")
            return producer
        except Exception:
            logger.warning(f"Kafka broker connection failed. Attempt {attempt+1}/5. Retrying in 3s...")
            time.sleep(3)
    raise ConnectionError("Catastrophic Failure: Could not bind cluster sockets to local Kafka infrastructure.")

def stream_to_lakehouse():
    """Continuous extraction loop piping raw live market metrics straight to stream topic blocks."""
    producer = initialize_producer()
    
    print("\n🚀 STARTING LIVE PRODUCTION STREAM INGESTION 🚀\n")
    
    while True:
        try:
            # Extract real messy data from the live external API endpoint
            response = requests.get(API_URL, timeout=10)
            raw_data = response.json()
            
            # Enrich our payload with a unified ingestion process timestamp
            payload = {
                "ingested_at": int(time.time()),
                "metrics": raw_data
            }
            
            # Broadcast the live telemetry into our Kafka buffer topic chunk
            producer.send(TOPIC_NAME, value=payload)
            logger.info(f"Successfully broadcast live payload slice to topic '{TOPIC_NAME}': {payload}")
            
            # Pause for 10 seconds to respect public API rate limits cleanly
            time.sleep(10)
            
        except KeyboardInterrupt:
            logger.info("Graceful shutdown initialized by operator. Flushing memory arrays...")
            break
        except Exception as e:
            logger.error(f"Upstream pipeline extraction anomaly encountered: {str(e)}. Retrying next interval...")
            time.sleep(5)
            
    producer.close()

if __name__ == "__main__":
    stream_to_lakehouse()