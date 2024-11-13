import requests
from kafka import KafkaProducer
import json
import time

API_KEY = "FXMGLC46AQCWXVE2"
KAFKA_TOPIC = "apple_news"
KAFKA_SERVER = "localhost:9092"

def fetch_data():
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['data']
    else:
        print("Error fetching data", response.status_code)
        return []

def produce_data():
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while True:
        news_data = fetch_data()
        for article in news_data:
            producer.send(KAFKA_TOPIC, article)
            time.sleep(1)  # Adjust based on rate of data generation
        time.sleep(60)  # Adjust frequency of API calls

if __name__ == "__main__":
    produce_data()
