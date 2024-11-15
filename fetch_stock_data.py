import requests
import json
from kafka import KafkaProducer
import time

# Configuration
API_KEY = "FXMGLC46AQCWXVE2"
KAFKA_TOPIC = "stock-stream"
KAFKA_SERVER = "localhost:9092"
ALPHAVANTAGE_URL = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey={API_KEY}"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_data_from_alphavantage():
    """Fetches news sentiment data from AlphaVantage API."""
    response = requests.get(ALPHAVANTAGE_URL)
    return response.json()

try:
    while True:
        data = fetch_data_from_alphavantage()
        producer.send(KAFKA_TOPIC, data)
        print(f"Sent data to Kafka (topic: {KAFKA_TOPIC}): {data}")
        time.sleep(60)  # Fetch data every 60 seconds (adjust as needed)
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.close()